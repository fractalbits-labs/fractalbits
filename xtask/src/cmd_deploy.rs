use crate::etcd_utils::download_etcd_for_deploy;
use crate::*;
use colored::*;
use dialoguer::Input;
use std::path::Path;

pub struct VpcConfig {
    pub template: Option<crate::VpcTemplate>,
    pub num_api_servers: u32,
    pub num_bench_clients: u32,
    pub num_bss_nodes: u32,
    pub with_bench: bool,
    pub bss_instance_type: String,
    pub api_server_instance_type: String,
    pub bench_client_instance_type: String,
    pub az: Option<String>,
    pub root_server_ha: bool,
    pub rss_backend: crate::RssBackend,
}

#[derive(Clone)]
struct CpuTarget {
    name: &'static str,
    arch: &'static str,
    rust_target: &'static str,
    rust_cpu: &'static str,
    zig_target: &'static str,
    zig_cpu: &'static str,
}

const CPU_TARGETS: &[CpuTarget] = &[
    CpuTarget {
        name: "graviton3",
        arch: "aarch64",
        rust_target: "aarch64-unknown-linux-gnu",
        rust_cpu: "neoverse-v1",
        zig_target: "aarch64-linux-gnu",
        zig_cpu: "neoverse_v1",
    },
    CpuTarget {
        name: "graviton4",
        arch: "aarch64",
        rust_target: "aarch64-unknown-linux-gnu",
        rust_cpu: "neoverse-v2",
        zig_target: "aarch64-linux-gnu",
        zig_cpu: "neoverse_v2",
    },
    CpuTarget {
        name: "i3",
        arch: "x86_64",
        rust_target: "x86_64-unknown-linux-gnu",
        rust_cpu: "", // for bss only, no rust binaries
        zig_target: "x86_64-linux-gnu",
        zig_cpu: "broadwell",
    },
    CpuTarget {
        name: "i3en",
        arch: "x86_64",
        rust_target: "x86_64-unknown-linux-gnu",
        rust_cpu: "", // for bss only, no rust binaries
        zig_target: "x86_64-linux-gnu",
        zig_cpu: "skylake",
    },
];

const RUST_BINS: &[&str] = &[
    "fractalbits-bootstrap",
    "root_server",
    "api_server",
    "nss_role_agent",
    "rss_admin",
    "rewrk_rpc",
];

const ZIG_BINS: &[&str] = &["nss_server", "bss_server", "test_fractal_art"];

pub fn build(
    deploy_target: DeployTarget,
    release_mode: bool,
    zig_extra_build: &[String],
    api_server_build_env: &[String],
) -> CmdResult {
    let (zig_build_opt, rust_build_opt, build_dir) = if release_mode {
        ("--release=safe", "--release", "release")
    } else {
        ("", "", "debug")
    };

    // Format Zig extra build options as -Dkey=value
    // Always include journal_atomic_write_size=16384 (16KB) for deployment
    let mut zig_build_with_defaults = vec!["journal_atomic_write_size=16384".to_string()];
    zig_build_with_defaults.extend_from_slice(zig_extra_build);
    let zig_extra_opts: &Vec<String> = &zig_build_with_defaults
        .iter()
        .map(|opt| format!("-D{}", opt))
        .collect();

    // Ensure required Rust targets are installed
    run_cmd! {
        info "Ensuring required Rust targets are installed";
        rustup target add x86_64-unknown-linux-gnu;
        rustup target add aarch64-unknown-linux-gnu;
    }?;

    // Create deploy directories for all CPU targets
    for target in CPU_TARGETS {
        let deploy_dir = format!("prebuilt/deploy/{}/{}", target.arch, target.name);
        run_cmd!(mkdir -p $deploy_dir)?;
    }

    // Build fractalbits-bootstrap separately for each architecture without CPU flags
    if matches!(
        deploy_target,
        DeployTarget::Bootstrap | DeployTarget::Rust | DeployTarget::All
    ) {
        let build_envs = cmd_build::get_build_envs();
        for arch in ["x86_64", "aarch64"] {
            let rust_target = format!("{arch}-unknown-linux-gnu");
            run_cmd! {
                info "Building fractalbits-bootstrap for $arch";
                $[build_envs] cargo zigbuild
                    -p fractalbits-bootstrap --target $rust_target $rust_build_opt;
            }?;

            // Copy fractalbits-bootstrap to arch-level directory
            let src_path = format!("target/{}/{}/fractalbits-bootstrap", rust_target, build_dir);
            let dst_path = format!("prebuilt/deploy/{}/fractalbits-bootstrap", arch);
            run_cmd! {
                mkdir -p prebuilt/deploy/$arch;
                cp $src_path $dst_path;
            }?;
        }
    }

    // Build other Rust projects with CPU-specific optimizations
    if matches!(deploy_target, DeployTarget::Rust | DeployTarget::All) {
        info!("Building Rust projects for all CPU targets");
        let build_envs = cmd_build::get_build_envs();

        for target in CPU_TARGETS {
            let rust_cpu = target.rust_cpu;
            let rust_target = target.rust_target;

            if rust_cpu.is_empty() {
                continue;
            }
            if api_server_build_env.is_empty() {
                run_cmd! {
                    info "Building Rust projects for $rust_target ($rust_cpu)";
                    RUSTFLAGS="-C target-cpu=$rust_cpu"
                    $[build_envs] cargo zigbuild
                        --target $rust_target $rust_build_opt --workspace
                        --exclude xtask
                        --exclude fractalbits-bootstrap;
                }?;
            } else {
                run_cmd! {
                    info "Building Rust projects for $rust_target ($rust_cpu)";
                    RUSTFLAGS="-C target-cpu=$rust_cpu"
                    $[build_envs] cargo zigbuild
                        --target $rust_target $rust_build_opt --workspace
                        --exclude xtask
                        --exclude fractalbits-bootstrap
                        --exclude api_server;

                    info "Building api_server ...";
                    RUSTFLAGS="-C target-cpu=$rust_cpu"
                    $[api_server_build_env] $[build_envs] cargo zigbuild
                        --target $rust_target $rust_build_opt
                        --package api_server;
                }?;
            }

            // Copy Rust binaries to deploy directory (excluding fractalbits-bootstrap)
            let deploy_dir = format!("prebuilt/deploy/{}/{}", target.arch, target.name);
            for bin in RUST_BINS {
                if *bin != "fractalbits-bootstrap" {
                    let src_path = format!("target/{}/{}/{}", rust_target, build_dir, bin);
                    let dst_path = format!("{}/{}", deploy_dir, bin);
                    if Path::new(&src_path).exists() {
                        run_cmd!(cp $src_path $dst_path)?;
                    }
                }
            }
        }
    }

    // Build Zig projects for all CPU targets
    if matches!(deploy_target, DeployTarget::Zig | DeployTarget::All)
        && Path::new(ZIG_REPO_PATH).exists()
    {
        info!("Building Zig projects for all CPU targets");
        let build_envs = cmd_build::get_build_envs();

        for target in CPU_TARGETS {
            let zig_out_dir = if target.arch == "aarch64" {
                format!("target/aarch64-unknown-linux-gnu/{build_dir}/zig-out")
            } else {
                format!("target/x86_64-unknown-linux-gnu/{build_dir}/zig-out")
            };

            let zig_target = target.zig_target;
            let zig_cpu = target.zig_cpu;
            run_cmd! {
                info "Building Zig projects for $zig_target ($zig_cpu)";
                cd $ZIG_REPO_PATH;
                $[build_envs] zig build
                    -p ../$zig_out_dir
                    -Dtarget=$zig_target -Dcpu=$zig_cpu $zig_build_opt $[zig_extra_opts] 2>&1;
            }?;

            // Copy Zig binaries to deploy directory
            let deploy_dir = format!("prebuilt/deploy/{}/{}", target.arch, target.name);
            for bin in ZIG_BINS {
                let src_path = format!("{}/bin/{}", zig_out_dir, bin);
                let dst_path = format!("{}/{}", deploy_dir, bin);
                run_cmd!(cp $src_path $dst_path)?;
            }
        }
    }

    // Build and copy UI
    if matches!(deploy_target, DeployTarget::Ui | DeployTarget::All)
        && Path::new(UI_REPO_PATH).exists()
    {
        let region = run_fun!(aws configure list | grep region | awk r"{print $2}")?;
        cmd_build::build_ui(&region)?;
        run_cmd! {
            rm -rf prebuilt/deploy/ui;
            cp -r ui/dist prebuilt/deploy/ui;
        }?;
    }

    // Download (extract) warp binary for each architecture
    if deploy_target == DeployTarget::All {
        download_warp_binaries()?;
        download_etcd_for_deploy()?;
    }

    info!("Deploy build is done");

    Ok(())
}

fn download_warp_binaries() -> CmdResult {
    for arch in ["x86_64", "aarch64"] {
        let linux_arch = if arch == "aarch64" { "arm64" } else { "x86_64" };

        let warp_version = "v1.3.0";
        let warp_file = format!("warp_Linux_{linux_arch}.tar.gz");
        let warp_path = format!("third_party/minio/{warp_file}");

        let base_url = "https://github.com/minio/warp/releases/download";
        let download_url = format!("{base_url}/{warp_version}/{warp_file}");
        let checksums_url = format!("{base_url}/{warp_version}/checksums.txt");

        // Check if already downloaded
        if !Path::new(&warp_path).exists() {
            run_cmd! {
                info "Downloading warp binary for $linux_arch";
                mkdir -p third_party/minio;
                curl -sL -o $warp_path $download_url;
            }?;
        }

        run_cmd! {
            cd third_party/minio;
            info "Verifying warp binary checksum for $linux_arch";
            curl -sL -o warp_checksums.txt $checksums_url;
            grep $warp_file warp_checksums.txt | sha256sum -c --quiet;
            rm -f warp_checksums.txt;
        }?;

        // Extract warp to arch-level directory
        let deploy_dir = format!("prebuilt/deploy/{}", arch);
        run_cmd! {
            info "Extracting warp binary to $deploy_dir for $linux_arch";
            tar -xzf third_party/minio/$warp_file -C $deploy_dir warp;
        }?;
    }

    Ok(())
}

pub fn upload() -> CmdResult {
    // Check/create S3 bucket and sync
    let bucket_name = get_bootstrap_bucket_name()?;
    let bucket = format!("s3://{bucket_name}");

    // Check if the bucket exists; create if it doesn't
    let bucket_exists = run_cmd!(aws s3api head-bucket --bucket $bucket_name &>/dev/null).is_ok();
    if !bucket_exists {
        run_cmd! {
            info "Creating bucket $bucket";
            aws s3 mb $bucket;
        }?;
    }

    run_cmd! {
        info "Syncing all binaries to S3 bucket $bucket";
        aws s3 sync prebuilt/deploy $bucket;
        info "Syncing all binaries is done";
    }?;
    Ok(())
}

fn get_bootstrap_bucket_name() -> FunResult {
    let region = run_fun!(aws configure get region)?;
    let account_id = run_fun!(aws sts get-caller-identity --query Account --output text)?;
    Ok(format!("fractalbits-bootstrap-{region}-{account_id}"))
}

pub fn destroy_vpc() -> CmdResult {
    // Display warning message
    warn!("This will permanently destroy the VPC and all associated resources!");
    warn!("This action cannot be undone.");

    // Require user to type exact confirmation text
    let _confirmation: String = Input::new()
        .with_prompt(format!(
            "Type {} to confirm VPC destruction",
            "permanent destroy".bold()
        ))
        .validate_with(|input: &String| -> Result<(), String> {
            if input == "permanent destroy" {
                Ok(())
            } else {
                Err(format!(
                    "You must type {} exactly to confirm",
                    "permanent destroy".bold()
                ))
            }
        })
        .interact_text()
        .map_err(|e| std::io::Error::other(format!("Failed to read confirmation: {e}")))?;

    // First destroy the CDK stack
    run_cmd! {
        info "Destroying CDK stack...";
        cd vpc/fractalbits-cdk;
        npx cdk destroy FractalbitsVpcStack 2>&1;
        info "CDK stack destroyed successfully";
    }?;

    // Then cleanup S3 bucket
    cleanup_bootstrap_bucket()?;

    info!("VPC destruction completed successfully");
    Ok(())
}

fn cleanup_bootstrap_bucket() -> CmdResult {
    let bucket_name = get_bootstrap_bucket_name()?;
    let bucket = format!("s3://{bucket_name}");

    // Check if the bucket exists
    let bucket_exists = run_cmd!(aws s3api head-bucket --bucket $bucket_name &>/dev/null).is_ok();
    if !bucket_exists {
        info!("Bucket {bucket} does not exist, nothing to clean up");
        return Ok(());
    }

    run_cmd! {
        info "Emptying bucket $bucket (delete all objects)";
        aws s3 rm $bucket --recursive;

        info "Deleting bucket $bucket";
        aws s3 rb $bucket;
        info "Successfully cleaned up builds bucket: $bucket";
    }?;

    Ok(())
}

pub fn create_vpc(config: VpcConfig) -> CmdResult {
    let VpcConfig {
        template,
        num_api_servers,
        num_bench_clients,
        num_bss_nodes,
        with_bench,
        bss_instance_type,
        api_server_instance_type,
        bench_client_instance_type,
        az,
        root_server_ha,
        rss_backend,
    } = config;

    // Note: Template-based configuration is handled in CDK (vpc/fractalbits-cdk/bin/fractalbits-vpc.ts)
    // The values passed here may be overridden by the template in CDK
    let cdk_dir = "vpc/fractalbits-cdk";

    // Check if node_modules exists, if not run npm install
    let node_modules_path = format!("{}/node_modules/", cdk_dir);
    if !Path::new(&node_modules_path).exists() {
        run_cmd! {
            info "Node modules not found. Installing dependencies...";
            cd $cdk_dir;
            npm install &>/dev/null;

            info "Disabling CDK collecting telemetry data...";
            npx cdk acknowledge 34892 &>/dev/null; // https://github.com/aws/aws-cdk/issues/34892
            npx cdk cli-telemetry --disable;
        }?;
    }

    // Check if CDK has been bootstrapped
    let bootstrap_cdk_exists = run_cmd! {
        aws cloudformation describe-stacks
            --stack-name CDKToolkit &>/dev/null
    }
    .is_ok();
    if !bootstrap_cdk_exists {
        run_cmd! {
            info "CDK bootstrap stack not found. Running CDK bootstrap...";
            cd $cdk_dir;
            npx cdk bootstrap 2>&1;
            info "CDK bootstrap completed successfully";
        }?;
    }

    // Build CDK context parameters (each --context flag and value must be separate arguments)
    let mut context_params = Vec::new();
    let mut add_context = |key: &str, value: String| {
        context_params.push("--context".to_string());
        context_params.push(format!("{}={}", key, value));
    };

    add_context("numApiServers", num_api_servers.to_string());
    add_context("numBenchClients", num_bench_clients.to_string());
    add_context("numBssNodes", num_bss_nodes.to_string());
    add_context("bssInstanceTypes", bss_instance_type);
    add_context("apiServerInstanceType", api_server_instance_type);
    add_context("benchClientInstanceType", bench_client_instance_type);
    if with_bench {
        add_context("benchType", "external".to_string());
    }
    if let Some(template_val) = template {
        add_context("vpcTemplate", template_val.as_ref().to_string());
    }
    if let Some(az_val) = az {
        add_context("az", az_val);
    }
    if root_server_ha {
        add_context("rootServerHa", "true".to_string());
    }
    add_context("rssBackend", rss_backend.as_ref().to_string());

    // Deploy the VPC stack
    run_cmd! {
        info "Deploying FractalbitsVpcStack...";
        cd $cdk_dir;
        npx cdk deploy FractalbitsVpcStack
            $[context_params]
            --require-approval never 2>&1;
        info "VPC deployment completed successfully";
    }?;

    Ok(())
}
