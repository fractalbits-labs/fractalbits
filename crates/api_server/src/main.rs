use actix_files::Files;
use actix_web::{App, HttpServer, middleware::Logger, rt::System, web};
use api_server::{AppState, CacheCoordinator, Config, api_key_routes, cache_mgmt, handler};
use clap::Parser;
use data_types::Versioned;
use rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use rustls_pemfile::{certs, private_key};
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::{fs::File, io::BufReader};
use tokio::signal::unix::{SignalKind, signal};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

#[derive(Parser)]
#[clap(name = "api_server", about = "API server")]
struct Opt {
    #[clap(short = 'c', long = "config", long_help = "Config file path")]
    config_file: Option<PathBuf>,
}

fn main() -> std::io::Result<()> {
    // AWS SDK suppression filter
    let third_party_filter = "tower_http=warn,hyper_util=warn,aws_smithy=warn,aws_sdk=warn,actix_web=warn,actix_server=warn,h2=warn";
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .map(|filter| {
                    format!("{filter},{third_party_filter}")
                        .parse()
                        .unwrap_or(filter)
                })
                .unwrap_or_else(|_| format!("info,{third_party_filter}").into()),
        )
        .with(tracing_subscriber::fmt::layer().without_time())
        .init();

    let main_build_info = option_env!("MAIN_BUILD_INFO").unwrap_or("unknown");
    let build_timestamp = option_env!("BUILD_TIMESTAMP").unwrap_or("unknown");
    let build_info = format!("{}, build time: {}", main_build_info, build_timestamp);
    eprintln!("build info: {}", build_info);

    let opt = Opt::parse();
    let mut config = match opt.config_file {
        Some(config_file) => config::Config::builder()
            .add_source(config::File::from(config_file).required(true))
            .add_source(config::Environment::with_prefix("APP"))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap(),
        None => {
            // Check for APP_BLOB_STORAGE_BACKEND environment variable
            if let Ok(backend) = std::env::var("APP_BLOB_STORAGE_BACKEND") {
                info!("APP_BLOB_STORAGE_BACKEND: {backend}");
                match backend.as_str() {
                    "s3_express_multi_az" => Config::s3_express_multi_az(),
                    "s3_hybrid_single_az" => Config::s3_hybrid_single_az(),
                    "all_in_bss_single_az" => Config::all_in_bss_single_az(),
                    _ => {
                        error!("Invalid APP_BLOB_STORAGE_BACKEND value: {backend}");
                        std::process::exit(1);
                    }
                }
            } else {
                config::Config::builder()
                    .add_source(config::Environment::with_prefix("APP"))
                    .build()
                    .unwrap()
                    .try_deserialize()
                    .unwrap_or_else(|_| Config::default())
            }
        }
    };

    if config.with_metrics {
        #[cfg(feature = "metrics_statsd")]
        {
            use metrics_exporter_statsd::StatsdBuilder;
            // Initialize StatsD metrics exporter
            let recorder = StatsdBuilder::from("127.0.0.1", 8125)
                .with_buffer_size(1)
                .build(None)
                .expect("Could not build StatsD recorder");
            metrics::set_global_recorder(Box::new(recorder))
                .expect("Could not install StatsD exporter");
            info!("Metrics exporter for StatsD installed");
        }
        #[cfg(feature = "metrics_prometheus")]
        {
            use metrics_exporter_prometheus::PrometheusBuilder;
            // Initialize Prometheus metrics exporter
            PrometheusBuilder::new()
                .with_http_listener("0.0.0.0:8085".parse::<SocketAddr>().unwrap())
                .install()
                .expect("Could not build Prometheus recorder");
            info!("Metrics exporter for Prometheus installed");
        }
    }

    let gui_web_root = std::env::var("GUI_WEB_ROOT").ok().map(PathBuf::from);
    if gui_web_root.is_some() {
        config.allow_missing_or_bad_signature = true;
    }

    let config = Arc::new(config);
    let port = config.port;
    let mgmt_port = config.mgmt_port;
    let mut https_config = config.https.clone();
    if std::env::var("HTTPS_DISABLED")
        .map(|v| v == "1")
        .unwrap_or(false)
    {
        https_config.enabled = false;
    }

    let worker_count = config.worker_threads;
    let worker_cores: Vec<_> = if config.set_thread_affinity {
        let all_cores = core_affinity::get_core_ids().expect("Failed to get core IDs");
        if worker_count > all_cores.len() {
            error!(
                "worker_threads ({}) exceeds available cores ({})",
                worker_count,
                all_cores.len()
            );
            std::process::exit(1);
        }
        all_cores.into_iter().take(worker_count).map(Some).collect()
    } else {
        vec![None; worker_count]
    };

    let cache_coordinator: Arc<CacheCoordinator<Versioned<String>>> =
        Arc::new(CacheCoordinator::new());
    let az_status_coordinator: Arc<CacheCoordinator<String>> = Arc::new(CacheCoordinator::new());

    let http_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
    let mgmt_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), mgmt_port);
    let https_addr = if https_config.enabled {
        Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            https_config.port,
        ))
    } else {
        None
    };

    info!(
        port,
        mgmt_port, worker_count, "Starting server with {worker_count} threads"
    );

    let stats_writer_handle = if config.enable_stats_writer {
        let stats_dir = config.stats_dir.clone();
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel::<()>();
        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to build stats writer runtime");

            rt.block_on(async {
                match api_server::unified_stats::init_unified_stats_writer(stats_dir).await {
                    Ok(mut writer) => {
                        info!("Unified stats writer initialized");
                        let shutdown_rx_async = tokio::task::spawn_blocking(move || {
                            let _ = shutdown_rx.recv();
                        });
                        let _ = shutdown_rx_async.await;
                        info!("Unified stats writer shutting down");
                        writer.stop().await;
                    }
                    Err(e) => {
                        error!("Failed to initialize unified stats writer: {}", e);
                    }
                }
            });
        });
        Some((handle, shutdown_tx))
    } else {
        None
    };

    let mut handles = Vec::with_capacity(worker_count);
    let mut server_handles = Vec::new();
    let (handle_tx, handle_rx) = std::sync::mpsc::channel();

    for (worker_idx, core_id) in worker_cores.into_iter().enumerate() {
        let http_listener = make_reuseport_listener(http_addr)?;
        let mgmt_listener = make_reuseport_listener(mgmt_addr)?;
        let https_listener = https_addr.map(make_reuseport_listener).transpose()?;

        let config = config.clone();
        let cache_coordinator = cache_coordinator.clone();
        let az_status_coordinator = az_status_coordinator.clone();
        let web_root = gui_web_root.clone();
        let https_config = https_config.clone();
        let handle_tx = handle_tx.clone();

        let handle = thread::Builder::new()
            .name(format!("actix-core-{worker_idx}"))
            .spawn(move || {
                if let Some(core_id) = core_id {
                    core_affinity::set_for_current(core_id);
                    info!(
                        worker_idx,
                        core_id = core_id.id,
                        "Worker thread pinned to core"
                    );
                }

                System::new().block_on(async move {
                    let app_state = Arc::new(AppState::new_per_core_sync(
                        config.clone(),
                        cache_coordinator,
                        az_status_coordinator,
                        worker_idx as u16,
                    ));

                    let mut server = HttpServer::new(move || {
                        if let Some(core_id) = core_id {
                            core_affinity::set_for_current(core_id);
                        }

                        let app_state = app_state.clone();
                        let mut app = App::new()
                            .app_data(web::Data::new(app_state))
                            .app_data(web::PayloadConfig::default().limit(5_368_709_120))
                            .wrap(Logger::default())
                            .service(
                                web::scope("/mgmt")
                                    .route("/health", web::get().to(cache_mgmt::mgmt_health))
                                    .route(
                                        "/cache/invalidate/bucket/{name}",
                                        web::post().to(cache_mgmt::invalidate_bucket),
                                    )
                                    .route(
                                        "/cache/invalidate/api_key/{id}",
                                        web::post().to(cache_mgmt::invalidate_api_key),
                                    )
                                    .route(
                                        "/cache/update/az_status/{id}",
                                        web::post().to(cache_mgmt::update_az_status),
                                    )
                                    .route("/cache/clear", web::post().to(cache_mgmt::clear_cache)),
                            )
                            .service(
                                web::scope("/api_keys")
                                    .route("/", web::post().to(api_key_routes::create_api_key))
                                    .route("/", web::get().to(api_key_routes::list_api_keys))
                                    .route(
                                        "/{key_id}",
                                        web::delete().to(api_key_routes::delete_api_key),
                                    ),
                            );

                        if let Some(ref web_root) = web_root {
                            let static_dir = web_root.clone();
                            app =
                                app.service(Files::new("/ui", static_dir).index_file("index.html"));
                        }

                        app.default_service(web::route().to(handler::any_handler))
                    });

                    server = server
                        .workers(1)
                        .max_connections(65536)
                        .max_connection_rate(65536)
                        .client_request_timeout(config.client_request_timeout())
                        .disable_signals();

                    server = server.listen(http_listener).unwrap();
                    server = server.listen(mgmt_listener).unwrap();

                    if let Some(https_listener) = https_listener {
                        let key_path = PathBuf::from(&https_config.key_file);
                        let private_key = match load_private_key(&key_path) {
                            Ok(private_key) => private_key,
                            Err(e) => {
                                error!(
                                    "Failed to load private key from {}: {e}",
                                    key_path.display()
                                );
                                std::process::exit(1);
                            }
                        };

                        let cert_path = PathBuf::from(&https_config.cert_file);
                        let cert_chain = match load_certificates(&cert_path) {
                            Ok(cert_chain) => cert_chain,
                            Err(e) => {
                                error!(
                                    "Failed to load certificate chain from {}: {e}",
                                    cert_path.display()
                                );
                                std::process::exit(1);
                            }
                        };

                        let mut tls_config = ServerConfig::builder_with_provider(Arc::new(
                            rustls::crypto::aws_lc_rs::default_provider(),
                        ))
                        .with_safe_default_protocol_versions()
                        .unwrap()
                        .with_no_client_auth()
                        .with_single_cert(cert_chain, private_key)
                        .unwrap();

                        if https_config.force_http1_only {
                            tls_config.alpn_protocols = vec![b"http/1.1".to_vec()];
                        } else {
                            tls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
                        }

                        server = server
                            .listen_rustls_0_23(https_listener, tls_config)
                            .unwrap();
                    }

                    let server = server.run();
                    let server_handle = server.handle();
                    let _ = handle_tx.send(server_handle);

                    server.await
                })
            })?;

        handles.push(handle);
    }

    drop(handle_tx);

    for server_handle in handle_rx {
        server_handles.push(server_handle);
    }
    let signal_handle = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to build signal handler runtime");

        rt.block_on(async {
            let mut sigterm =
                signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
            let mut sigint =
                signal(SignalKind::interrupt()).expect("Failed to register SIGINT handler");

            tokio::select! {
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, shutting down gracefully");
                }
                _ = sigint.recv() => {
                    info!("Received SIGINT, shutting down gracefully");
                }
            }

            SHUTDOWN.store(true, Ordering::Release);

            for server_handle in server_handles {
                server_handle.stop(true).await;
            }
            info!("All servers stopped");
        });
    });

    for (idx, handle) in handles.into_iter().enumerate() {
        if let Err(e) = handle.join() {
            error!("Worker thread {idx} panicked: {e:?}");
        }
    }

    if let Err(e) = signal_handle.join() {
        error!("Signal handler thread panicked: {e:?}");
    }

    if let Some((stats_writer_handle, shutdown_tx)) = stats_writer_handle {
        info!("All worker threads exited, shutting down stats writer");
        let _ = shutdown_tx.send(());
        if let Err(e) = stats_writer_handle.join() {
            error!("Stats writer thread panicked: {e:?}");
        }
    }

    Ok(())
}

fn make_reuseport_listener(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let domain = Domain::for_address(addr);
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;

    socket.set_nodelay(true)?;
    socket.set_recv_buffer_size(16 * 1024 * 1024)?;
    socket.set_send_buffer_size(16 * 1024 * 1024)?;

    socket.bind(&addr.into())?;
    socket.listen(65536)?;

    let listener: TcpListener = socket.into();
    listener.set_nonblocking(true)?;
    Ok(listener)
}

fn load_private_key(
    key_path: &PathBuf,
) -> Result<PrivateKeyDer<'static>, Box<dyn std::error::Error>> {
    let file = File::open(key_path)?;
    let mut reader = BufReader::new(file);
    let key = private_key(&mut reader)?
        .ok_or_else(|| format!("No private key found in {}", key_path.display()))?;
    Ok(key)
}

fn load_certificates(
    cert_path: &PathBuf,
) -> Result<Vec<CertificateDer<'static>>, Box<dyn std::error::Error>> {
    let file = File::open(cert_path)?;
    let mut reader = BufReader::new(file);
    let cert_chain = certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
    if cert_chain.is_empty() {
        return Err(format!("No certificates found in {}", cert_path.display()).into());
    }
    Ok(cert_chain)
}
