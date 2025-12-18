use crate::*;
use cmd_lib::*;
use std::io::Error;
use std::path::Path;

const DEFAULT_CONTAINER_NAME: &str = "fractalbits-dev";

pub fn run_cmd_docker(cmd: DockerCommand) -> CmdResult {
    match cmd {
        DockerCommand::Build {
            release,
            prebuilt,
            image_name,
            tag,
        } => build_docker_image(release, prebuilt, &image_name, &tag),
        DockerCommand::Run {
            image_name,
            tag,
            port,
            name,
            detach,
        } => run_docker_container(&image_name, &tag, port, name.as_deref(), detach),
        DockerCommand::Stop { name } => stop_docker_container(name.as_deref()),
        DockerCommand::Logs { name, follow } => show_docker_logs(name.as_deref(), follow),
    }
}

fn build_docker_image(release: bool, prebuilt: bool, image_name: &str, tag: &str) -> CmdResult {
    info!("Building Docker image: {}:{}", image_name, tag);

    let target_dir = if release {
        "target/release"
    } else {
        "target/debug"
    };
    let staging_dir = "target/docker-staging";
    let bin_staging = format!("{}/bin", staging_dir);

    if prebuilt {
        info!("Using prebuilt binaries from prebuilt/ directory");
        info!("Building api_server and docker-all-in-one only...");
        if release {
            run_cmd!(cargo build --release -p api_server -p docker-all-in-one)?;
        } else {
            run_cmd!(cargo build -p api_server -p docker-all-in-one)?;
        }
    } else {
        info!("Building all binaries...");
        cmd_build::build_all(release)?;

        info!("Building docker-all-in-one...");
        if release {
            run_cmd!(cargo build --release -p docker-all-in-one)?;
        } else {
            run_cmd!(cargo build -p docker-all-in-one)?;
        }
    }

    info!("Ensuring etcd binary...");
    ensure_etcd()?;

    info!("Preparing staging directory...");
    run_cmd! {
        rm -rf $staging_dir;
        mkdir -p $bin_staging;
    }?;

    // Copy binaries built in this repo
    let local_binaries = ["api_server", "docker-all-in-one"];
    for bin in &local_binaries {
        let src = format!("{}/{}", target_dir, bin);
        if Path::new(&src).exists() {
            run_cmd!(cp $src $bin_staging/)?;
        } else {
            return Err(Error::other(format!("Binary not found: {}", src)));
        }
    }

    if prebuilt {
        // Use prebuilt binaries for external repos
        let prebuilt_binaries = ["root_server", "rss_admin", "bss_server", "nss_server"];
        for bin in &prebuilt_binaries {
            let src = format!("prebuilt/{}", bin);
            if Path::new(&src).exists() {
                run_cmd!(cp $src $bin_staging/)?;
            } else {
                return Err(Error::other(format!("Prebuilt binary not found: {}", src)));
            }
        }
    } else {
        // Use freshly built binaries
        let rust_binaries = ["root_server", "rss_admin"];
        for bin in &rust_binaries {
            let src = format!("{}/{}", target_dir, bin);
            if Path::new(&src).exists() {
                run_cmd!(cp $src $bin_staging/)?;
            } else {
                return Err(Error::other(format!("Rust binary not found: {}", src)));
            }
        }

        let zig_out = if release {
            crate::ZIG_RELEASE_OUT
        } else {
            crate::ZIG_DEBUG_OUT
        };
        let zig_binaries = ["bss_server", "nss_server"];
        for bin in &zig_binaries {
            let src = format!("{}/bin/{}", zig_out, bin);
            if Path::new(&src).exists() {
                run_cmd!(cp $src $bin_staging/)?;
            } else {
                return Err(Error::other(format!("Zig binary not found: {}", src)));
            }
        }
    }

    run_cmd! {
        cp third_party/etcd/etcd $bin_staging/;
        cp third_party/etcd/etcdctl $bin_staging/;
    }?;

    write_dockerfile(staging_dir)?;

    let dockerfile_path = format!("{}/Dockerfile", staging_dir);
    let image_id = run_fun! {
        docker build -q -t "${image_name}:${tag}" -f $dockerfile_path $staging_dir
    }?;
    let short_id = image_id
        .trim()
        .trim_start_matches("sha256:")
        .chars()
        .take(12)
        .collect::<String>();

    info!("Docker image built: {}:{} ({})", image_name, tag, short_id);
    Ok(())
}

fn run_docker_container(
    image_name: &str,
    tag: &str,
    port: u16,
    name: Option<&str>,
    detach: bool,
) -> CmdResult {
    let container_name = name.unwrap_or(DEFAULT_CONTAINER_NAME);
    let image = format!("{}:{}", image_name, tag);

    info!(
        "Running Docker container: {} (port: {})",
        container_name, port
    );

    let port_mapping = format!("{}:8080", port);
    let mgmt_port_mapping = "18080:18080";
    if detach {
        run_cmd!(docker run -d --privileged --name $container_name -p $port_mapping -p $mgmt_port_mapping -v "fractalbits-data:/data" $image)?;
        info!("Container started in detached mode: {}", container_name);
    } else {
        run_cmd!(docker run --rm --privileged --name $container_name -p $port_mapping -p $mgmt_port_mapping -v "fractalbits-data:/data" $image)?;
    }

    Ok(())
}

fn stop_docker_container(name: Option<&str>) -> CmdResult {
    let container_name = name.unwrap_or(DEFAULT_CONTAINER_NAME);
    info!("Stopping Docker container: {}", container_name);

    run_cmd!(docker stop $container_name)?;
    let _ = run_cmd!(docker rm $container_name 2>/dev/null);

    info!("Container stopped: {}", container_name);
    Ok(())
}

fn show_docker_logs(name: Option<&str>, follow: bool) -> CmdResult {
    let container_name = name.unwrap_or(DEFAULT_CONTAINER_NAME);

    if follow {
        run_cmd!(docker logs -f $container_name)?;
    } else {
        run_cmd!(docker logs $container_name)?;
    }

    Ok(())
}

fn ensure_etcd() -> CmdResult {
    let etcd_dir = "third_party/etcd";
    let etcd_path = format!("{etcd_dir}/etcd");

    if Path::new(&etcd_path).exists() {
        return Ok(());
    }

    let etcd_version = "v3.6.7";
    let etcd_tarball = format!("etcd-{etcd_version}-linux-amd64.tar.gz");
    let download_url =
        format!("https://github.com/etcd-io/etcd/releases/download/{etcd_version}/{etcd_tarball}");
    let tarball_path = format!("third_party/{etcd_tarball}");

    if !Path::new(&tarball_path).exists() {
        run_cmd! {
            info "Downloading etcd binary...";
            mkdir -p third_party;
            curl -L -o $tarball_path $download_url 2>&1;
        }?;
    }

    let extracted_dir = format!("third_party/etcd-{}-linux-amd64", etcd_version);
    run_cmd! {
        info "Extracting etcd...";
        mkdir -p $etcd_dir;
        tar xzf $tarball_path -C third_party;
        mv $extracted_dir/etcd $etcd_dir/;
        mv $extracted_dir/etcdctl $etcd_dir/;
        rm -rf $extracted_dir;
    }?;

    Ok(())
}

fn write_dockerfile(staging_dir: &str) -> CmdResult {
    let dockerfile_content = r#"FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/fractalbits/bin /opt/fractalbits/etc /data

COPY bin/ /opt/fractalbits/bin/

RUN chmod +x /opt/fractalbits/bin/*

ENV PATH="/opt/fractalbits/bin:$PATH"
ENV RUST_LOG=info

EXPOSE 8080 18080 2379

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -sf http://localhost:18080/mgmt/health || exit 1

VOLUME /data

ENTRYPOINT ["docker-all-in-one"]
CMD ["--bin-dir=/opt/fractalbits/bin", "--data-dir=/data"]
"#;

    let dockerfile_path = format!("{}/Dockerfile", staging_dir);
    std::fs::write(&dockerfile_path, dockerfile_content)?;
    info!("Wrote Dockerfile to {}", dockerfile_path);

    Ok(())
}
