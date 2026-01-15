use crate::cmd_build::get_build_envs;
use crate::etcd_utils::{ensure_etcd_local, resolve_etcd_bin};
use crate::*;
use cmd_lib::*;
use std::io::Error;

const DEFAULT_CONTAINER_NAME: &str = "fractalbits-dev";

pub fn run_cmd_docker(cmd: DockerCommand) -> CmdResult {
    match cmd {
        DockerCommand::Build {
            release,
            all_from_source,
            image_name,
            tag,
        } => build_docker_image(release, all_from_source, &image_name, &tag),
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

fn build_docker_image(
    release: bool,
    all_from_source: bool,
    image_name: &str,
    tag: &str,
) -> CmdResult {
    info!("Building Docker image: {}:{}", image_name, tag);

    let build_envs = get_build_envs();
    let (build_flag, target_dir) = if release {
        ("--release", "target/release")
    } else {
        ("", "target/debug")
    };
    let staging_dir = "target/docker-staging";
    let bin_staging = format!("{staging_dir}/bin");

    if all_from_source {
        info!("Building binaries for Docker...");
        cmd_build::build_for_docker(release)?;
    } else {
        info!("Using prebuilt binaries from prebuilt/ directory");
        run_cmd!($[build_envs] cargo build $build_flag -p api_server -p container-all-in-one)?;
    }

    info!("Ensuring etcd binary...");
    ensure_etcd_local()?;

    info!("Preparing staging directory...");
    run_cmd! {
        rm -rf $staging_dir;
        mkdir -p $bin_staging;
    }?;

    // Copy binaries built in this repo
    let local_rust_binaries = ["api_server", "container-all-in-one"];
    for rust_bin in &local_rust_binaries {
        run_cmd!(cp $target_dir/$rust_bin $bin_staging/)?;
    }

    if all_from_source {
        // Use freshly built binaries, fall back to prebuilt if not found
        let rust_binaries = ["root_server", "rss_admin", "nss_role_agent"];
        for bin in &rust_binaries {
            let bin_path = format!("{target_dir}/{bin}");
            if std::path::Path::new(&bin_path).exists() {
                run_cmd!(cp $bin_path $bin_staging/)?;
            } else {
                info!("{} not found, using prebuilt", bin);
                run_cmd!(cp prebuilt/$bin $bin_staging/)?;
            }
        }

        let zig_bin_dir = format!("{target_dir}/zig-out/bin");
        let zig_binaries = ["bss_server", "nss_server"];
        for zig_bin in &zig_binaries {
            let bin_path = format!("{zig_bin_dir}/{zig_bin}");
            if std::path::Path::new(&bin_path).exists() {
                run_cmd!(cp $bin_path $bin_staging/)?;
            } else {
                info!("{} not found, using prebuilt", zig_bin);
                run_cmd!(cp prebuilt/$zig_bin $bin_staging/)?;
            }
        }
    } else {
        // Use prebuilt binaries for external repos
        let prebuilt_binaries = [
            "root_server",
            "rss_admin",
            "bss_server",
            "nss_server",
            "nss_role_agent",
        ];
        for bin in &prebuilt_binaries {
            run_cmd!(cp prebuilt/$bin $bin_staging/)?;
        }
    }

    for bin in ["etcd", "etcdctl"] {
        let bin_path = resolve_etcd_bin(bin);
        run_cmd!(cp $bin_path $bin_staging/)?;
    }

    write_dockerfile(staging_dir)?;

    let dockerfile_path = format!("{}/Dockerfile", staging_dir);
    let image_id = run_fun! {
        docker build --no-cache -q -t "${image_name}:${tag}" -f $dockerfile_path $staging_dir
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

    info!("=== Running Docker container: {container_name} (port: {port}) ===");

    run_cmd! {
        info "Clean up any existing container";
        ignore docker stop $container_name 2>/dev/null;
        ignore docker rm -f $container_name 2>/dev/null;
    }?;

    let port_mapping = format!("{}:8080", port);
    let mgmt_port_mapping = "18080:18080";
    if detach {
        run_cmd!(docker run -d --privileged --name $container_name -p $port_mapping -p $mgmt_port_mapping -v "fractalbits-data:/data" $image)?;
        info!("Container started in detached mode: {}", container_name);
    } else {
        // Use std::process::Command for interactive mode to pass through raw output
        let status = std::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                "--privileged",
                "--name",
                container_name,
                "-p",
                &port_mapping,
                "-p",
                mgmt_port_mapping,
                "-v",
                "fractalbits-data:/data",
                &image,
            ])
            .status()?;
        if !status.success() {
            return Err(Error::other(format!(
                "docker run failed with exit code: {:?}",
                status.code()
            )));
        }
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

HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=3 \
    CMD curl -sf http://localhost:18080/mgmt/health || exit 1

VOLUME /data

ENTRYPOINT ["container-all-in-one"]
CMD ["--bin-dir=/opt/fractalbits/bin", "--data-dir=/data"]
"#;

    let dockerfile_path = format!("{}/Dockerfile", staging_dir);
    std::fs::write(&dockerfile_path, dockerfile_content)?;
    info!("Wrote Dockerfile to {}", dockerfile_path);

    Ok(())
}
