use crate::cmd_build::get_build_envs;
use crate::docker_build::{
    BinarySources, DockerBuildConfig, build_docker_image, get_host_arch, stage_binaries_for_docker,
};
use crate::etcd_utils::{ensure_etcd_local, resolve_etcd_dir};
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
        } => build_local_docker_image(release, all_from_source, &image_name, &tag, true),
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

fn build_local_docker_image(
    release: bool,
    all_from_source: bool,
    image_name: &str,
    tag: &str,
    include_volume: bool,
) -> CmdResult {
    info!("Building Docker image: {}:{}", image_name, tag);

    let build_envs = get_build_envs();
    let (build_flag, target_dir) = if release {
        ("--release", "target/release")
    } else {
        ("", "target/debug")
    };
    let staging_dir = "target/docker-staging";
    let arch = get_host_arch();
    let prebuilt_dir = format!("prebuilt/dev/{}", arch);

    // Build binaries
    if all_from_source {
        info!("Building binaries for Docker...");
        cmd_build::build_for_docker(release)?;
    } else {
        info!("Using prebuilt binaries from prebuilt/ directory");
        run_cmd!($[build_envs] cargo build $build_flag -p api_server -p container-all-in-one)?;
    }

    // Ensure etcd is available
    info!("Ensuring etcd binary...");
    ensure_etcd_local()?;

    // Prepare staging directory
    info!("Preparing staging directory...");
    run_cmd!(rm -rf $staging_dir)?;

    // Stage binaries
    let zig_bin_dir = format!("{}/zig-out/bin", target_dir);
    let sources = BinarySources {
        rust_bin_dir: target_dir,
        zig_bin_dir: if all_from_source {
            Some(zig_bin_dir.as_str())
        } else {
            None
        },
        prebuilt_dir: &prebuilt_dir,
        etcd_dir: &resolve_etcd_dir(),
        prefer_built: all_from_source,
    };
    stage_binaries_for_docker(&sources, staging_dir, "bin")?;

    // Build Docker image
    let config = DockerBuildConfig {
        image_name,
        tag,
        arch: Some(&arch),
        platform: None,
        staging_dir,
        bin_subdir: "bin",
        include_volume,
        data_source: None,
    };
    build_docker_image(&config)
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
