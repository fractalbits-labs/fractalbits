mod cmd_bench;
mod cmd_build;
mod cmd_deploy;
mod cmd_docker;
mod cmd_nightly;
mod cmd_prebuilt;
mod cmd_precheckin;
mod cmd_repo;
mod cmd_run_tests;
mod cmd_service;
mod cmd_tool;
mod etcd_utils;

use clap::{ArgAction, Parser};
use cmd_build::BuildMode;
use cmd_lib::*;
use strum::{AsRefStr, EnumString};

pub const TS_FMT: &str = "%b %d %H:%M:%.S";
// Need to match with api_server's default config to make authentication work
pub const UI_DEFAULT_REGION: &str = "localdev";
pub const UI_REPO_PATH: &str = "ui";
pub const ZIG_DEBUG_OUT: &str = "target/debug/zig-out";
pub const ZIG_RELEASE_OUT: &str = "target/release/zig-out";
pub const ZIG_REPO_PATH: &str = "core";

#[derive(Parser)]
#[clap(name = "xtask", about = "Misc project related tasks")]
enum Cmd {
    #[clap(about = "Run benchmark")]
    Bench {
        #[clap(long, default_value = "write", value_enum)]
        workload: BenchWorkload,

        #[clap(long, long_help = "Run with perf tool and generate flamegraph")]
        with_flame_graph: bool,

        #[clap(
            long,
            long_help = "set max number of keys for benchmark",
            default_value = "5000000"
        )]
        keys_limit: usize,

        #[clap(value_enum)]
        service: BenchService,
    },

    #[clap(about = "Run nightly tests")]
    Nightly,

    #[clap(about = "Run precheckin tests")]
    Precheckin {
        #[clap(long, long_help = "Run s3 api tests only")]
        s3_api_only: bool,

        #[clap(long, long_help = "Run zig unit tests only")]
        zig_unit_tests_only: bool,

        #[clap(
            long,
            long_help = "Debug by recompiling and restarting api_server only"
        )]
        debug_api_server: bool,

        #[clap(long, long_help = "Run fractal art tests in addition to other tests")]
        with_fractal_art_tests: bool,

        #[clap(long, long_help = "Enable HTTPS tests")]
        with_https: bool,

        #[clap(long, value_enum)]
        #[arg(default_value_t)]
        data_blob_storage: DataBlobStorage,

        #[clap(
            long,
            value_enum,
            long_help = "Docker test mode: included (default), excluded, or only"
        )]
        #[arg(default_value_t)]
        docker: DockerTestMode,
    },

    #[clap(about = "Build the whole project")]
    Build {
        #[clap(subcommand)]
        command: Option<BuildCommand>,

        #[clap(long, long_help = "release build or not")]
        release: bool,
    },

    #[clap(about = "Service stop/init/start/restart")]
    #[command(subcommand)]
    Service(ServiceCommand),

    #[clap(about = "Run tool related commands")]
    #[command(subcommand)]
    Tools(ToolKind),

    #[clap(about = "Deploy binaries to s3 builds bucket")]
    #[clap(subcommand)]
    Deploy(DeployCommand),

    #[clap(about = "Run various test suites")]
    RunTests {
        #[clap(subcommand)]
        test_type: Option<TestType>,
    },

    #[clap(about = "Git repos management commands")]
    #[command(subcommand)]
    Repo(RepoCommand),

    #[clap(about = "Docker image build and run commands")]
    #[command(subcommand)]
    Docker(DockerCommand),

    #[clap(about = "Prebuilt binaries management commands")]
    #[command(subcommand)]
    Prebuilt(PrebuiltCommand),
}

#[derive(Parser, Clone)]
pub enum DockerCommand {
    #[clap(about = "Build Docker image")]
    Build {
        #[clap(long, long_help = "release build or not")]
        release: bool,

        #[clap(
            long,
            long_help = "Build all binaries from source instead of using prebuilt"
        )]
        all_from_source: bool,

        #[clap(long, default_value = "fractalbits", long_help = "Docker image name")]
        image_name: String,

        #[clap(long, default_value = "latest", long_help = "Docker image tag")]
        tag: String,
    },

    #[clap(about = "Run Docker container")]
    Run {
        #[clap(long, default_value = "fractalbits", long_help = "Docker image name")]
        image_name: String,

        #[clap(long, default_value = "latest", long_help = "Docker image tag")]
        tag: String,

        #[clap(long, default_value = "8080", long_help = "Host port for S3 API")]
        port: u16,

        #[clap(long, long_help = "Container name")]
        name: Option<String>,

        #[clap(long, long_help = "Run in detached mode")]
        detach: bool,
    },

    #[clap(about = "Stop Docker container")]
    Stop {
        #[clap(long, long_help = "Container name to stop")]
        name: Option<String>,
    },

    #[clap(about = "Show Docker container logs")]
    Logs {
        #[clap(long, long_help = "Container name")]
        name: Option<String>,

        #[clap(long, long_help = "Follow log output")]
        follow: bool,
    },
}

#[derive(Parser, Clone)]
pub enum PrebuiltCommand {
    #[clap(about = "Publish prebuilt binaries to remote repository")]
    Publish {
        #[clap(long, long_help = "Skip the build step (use existing binaries)")]
        skip_build: bool,

        #[clap(long, long_help = "Dry run - don't commit or push")]
        dry_run: bool,

        #[clap(
            long,
            long_help = "Allow publishing even if some repos have uncommitted changes"
        )]
        allow_dirty: bool,
    },

    #[clap(about = "Update prebuilt binaries to latest version (shallow clone)")]
    Update,
}

#[derive(Parser, Clone)]
pub enum BuildCommand {
    #[clap(about = "Build all components")]
    All,
    #[clap(about = "Build only zig components")]
    Zig {
        #[clap(subcommand)]
        command: Option<ZigCommand>,
    },
    #[clap(about = "Build only rust components")]
    Rust,
    #[clap(about = "Build prebuilt binaries")]
    Prebuilt,
}

#[derive(Parser, Clone)]
pub enum ZigCommand {
    #[clap(about = "Run zig unit tests")]
    Test,
}

#[derive(Parser, Clone)]
pub enum DeployCommand {
    #[clap(about = "Build binaries for deployment")]
    Build {
        #[clap(long, default_value = "all", value_enum)]
        target: DeployTarget,

        #[clap(long, action=ArgAction::Set, default_value = "true", num_args = 0..=1)]
        release: bool,

        #[clap(
            long,
            value_delimiter = ',',
            long_help = "Zig extra build options in format: key=value,key2=value2"
        )]
        zig_extra_build: Vec<String>,

        #[clap(
            long,
            value_delimiter = ',',
            long_help = "Api server extra build environment variables in format: KEY=value,KEY2=value2"
        )]
        api_server_build_env: Vec<String>,
    },

    #[clap(about = "Upload prebuilt binaries to s3 builds bucket")]
    Upload {
        #[clap(long, value_enum, default_value = "aws")]
        deploy_target: xtask_common::DeployTarget,
    },

    #[clap(about = "Create VPC infrastructure using CDK")]
    CreateVpc {
        #[clap(
            long,
            value_enum,
            long_help = "VPC deployment template (mini or perf_demo)"
        )]
        template: Option<VpcTemplate>,

        #[clap(long, long_help = "Number of API servers", default_value = "1")]
        num_api_servers: u32,

        #[clap(long, long_help = "Number of benchmark clients", default_value = "1")]
        num_bench_clients: u32,

        #[clap(long, long_help = "Number of BSS nodes", default_value = "1")]
        num_bss_nodes: u32,

        #[clap(long, long_help = "Enable external benchmark mode")]
        with_bench: bool,

        #[clap(long, long_help = "BSS instance type", default_value = "i8g.2xlarge")]
        bss_instance_type: String,

        #[clap(
            long,
            long_help = "API server instance type",
            default_value = "c8g.xlarge"
        )]
        api_server_instance_type: String,

        #[clap(
            long,
            long_help = "Benchmark client instance type",
            default_value = "c8g.xlarge"
        )]
        bench_client_instance_type: String,

        #[clap(long, long_help = "Availability zone ID (e.g., usw2-az3, use1-az4)")]
        az: Option<String>,

        #[clap(
            long,
            long_help = "Enable root server high availability (2 RSS instances)"
        )]
        root_server_ha: bool,

        #[clap(
            long,
            value_enum,
            long_help = "RSS backend storage (ddb or etcd)",
            default_value = "ddb"
        )]
        rss_backend: RssBackend,

        #[clap(long, long_help = "Bootstrap via SSM instead of userData")]
        ssm_bootstrap: bool,

        #[clap(
            long,
            value_enum,
            long_help = "Journal type (ebs or nvme)",
            default_value = "ebs"
        )]
        journal_type: JournalType,

        #[clap(long, long_help = "Watch bootstrap progress inline after VPC creation")]
        watch_bootstrap: bool,
    },

    #[clap(about = "Destroy VPC infrastructure (including s3 builds bucket cleanup)")]
    DestroyVpc,

    #[clap(about = "Show bootstrap progress for a VPC deployment")]
    BootstrapProgress {
        #[clap(long, value_enum, default_value = "aws")]
        deploy_target: xtask_common::DeployTarget,
    },

    #[clap(about = "Create cluster from a cluster.toml config file")]
    CreateCluster {
        #[clap(long, long_help = "Path to cluster.toml config file")]
        config: String,

        #[clap(long, long_help = "Bootstrap S3 endpoint URL (e.g., 10.0.0.1:8080)")]
        bootstrap_s3_url: String,

        #[clap(
            long,
            long_help = "Watch bootstrap progress inline after cluster creation"
        )]
        watch_bootstrap: bool,
    },
}

#[derive(Clone, AsRefStr, EnumString, clap::ValueEnum)]
enum BenchWorkload {
    Read,
    Write,
}

#[derive(Clone, EnumString, clap::ValueEnum)]
enum BenchService {
    NssRpc,
    BssRpc,
}

#[derive(Parser, Clone, EnumString, PartialEq)]
pub enum ServiceAction {
    Init,
    Stop,
    Start,
    Restart,
    Status,
}

#[derive(AsRefStr, EnumString, Copy, Clone, PartialEq, clap::ValueEnum)]
#[strum(serialize_all = "snake_case")]
#[clap(rename_all = "snake_case")]
pub enum ServiceName {
    GuiServer,
    ApiServer,
    Bss,
    NssRoleAgentA,
    Nss,
    NssRoleAgentB,
    Mirrord,
    Rss,
    All,
    Minio,
    MinioAz1,
    MinioAz2,
    DdbLocal,
    Etcd,
}

impl ServiceName {
    pub fn is_bss(&self) -> bool {
        matches!(self, ServiceName::Bss)
    }
}

#[derive(AsRefStr, EnumString, Copy, Clone, Default, PartialEq, clap::ValueEnum)]
#[strum(serialize_all = "snake_case")]
#[clap(rename_all = "snake_case")]
pub enum DataBlobStorage {
    S3HybridSingleAz,
    S3ExpressMultiAz,
    #[default]
    AllInBssSingleAz,
}

#[derive(AsRefStr, EnumString, Copy, Clone, Default, PartialEq, clap::ValueEnum)]
#[strum(serialize_all = "snake_case")]
#[clap(rename_all = "snake_case")]
pub enum DockerTestMode {
    #[default]
    Included,
    Excluded,
    Only,
}

#[derive(AsRefStr, EnumString, Copy, Clone, Default, PartialEq, clap::ValueEnum)]
#[strum(serialize_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum RssBackend {
    Ddb,
    #[default]
    Etcd,
}

#[derive(AsRefStr, EnumString, Copy, Clone, Default, PartialEq, clap::ValueEnum)]
pub enum DeployTarget {
    Zig,
    Rust,
    Bootstrap,
    Ui,
    #[default]
    All,
}

#[derive(AsRefStr, EnumString, Copy, Clone, PartialEq, Debug, clap::ValueEnum)]
#[strum(serialize_all = "snake_case")]
#[clap(rename_all = "snake_case")]
pub enum VpcTemplate {
    Mini,
    PerfDemo,
}

#[derive(AsRefStr, EnumString, Copy, Clone, Default)]
pub enum NssRole {
    #[default]
    Active,
    Solo,
}

#[derive(AsRefStr, EnumString, Copy, Clone, Default, PartialEq, clap::ValueEnum)]
#[strum(serialize_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum JournalType {
    #[default]
    Ebs,
    Nvme,
}

#[derive(Copy, Clone)]
pub struct InitConfig {
    pub for_gui: bool,
    pub data_blob_storage: DataBlobStorage,
    pub with_https: bool,
    pub bss_count: u32,
    pub nss_disable_restart_limit: bool,
    pub rss_backend: RssBackend,
    pub journal_type: JournalType,
}

impl Default for InitConfig {
    fn default() -> Self {
        Self {
            for_gui: false,
            data_blob_storage: Default::default(),
            with_https: false,
            bss_count: 1,
            nss_disable_restart_limit: false,
            rss_backend: Default::default(),
            journal_type: Default::default(),
        }
    }
}

#[derive(Parser, Clone)]
pub enum ServiceCommand {
    Init {
        #[clap(default_value = "all", value_enum)]
        service: ServiceName,

        #[clap(long, long_help = "release build or not")]
        release: bool,

        #[clap(long, long_help = "start service for gui")]
        for_gui: bool,

        #[clap(long, value_enum)]
        #[arg(default_value_t)]
        data_blob_storage: DataBlobStorage,

        #[clap(long, long_help = "enable HTTPS certificates generation")]
        with_https: bool,

        #[clap(
            long,
            long_help = "number of BSS services to create (must be 1 or 6)",
            default_value = "1"
        )]
        bss_count: u32,

        #[clap(long, long_help = "disable restart limit for NSS role agent")]
        nss_disable_restart_limit: bool,

        #[clap(long, value_enum)]
        #[arg(default_value_t)]
        rss_backend: RssBackend,

        #[clap(long, value_enum, long_help = "journal type (ebs or nvme)")]
        #[arg(default_value_t)]
        journal_type: JournalType,
    },
    Stop {
        #[clap(default_value = "all", value_enum)]
        service: ServiceName,
    },
    Start {
        #[clap(default_value = "all", value_enum)]
        service: ServiceName,
    },
    Restart {
        #[clap(default_value = "all", value_enum)]
        service: ServiceName,
    },
    Status {
        #[clap(default_value = "all", value_enum)]
        service: ServiceName,
    },
}

#[derive(Parser, Clone)]
pub enum TestType {
    All,
    MultiAz {
        #[clap(subcommand)]
        subcommand: MultiAzTestType,
    },
    LeaderElection,
    BssNodeFailure,
    NssHaFailover,
}

#[derive(Parser, Clone, EnumString)]
pub enum MultiAzTestType {
    All,
    DataBlobTracking,
    DataBlobResyncing,
}

#[derive(Parser, Clone)]
pub enum RepoCommand {
    #[clap(about = "List all configured git repos")]
    List,

    #[clap(about = "Show git repo status")]
    Status,

    #[clap(about = "Initialize all git repos")]
    Init {
        #[clap(long, long_help = "Initialize all repos (including private ones)")]
        all: bool,
    },

    #[clap(about = "Run a command in each git repo")]
    Foreach {
        #[clap(required = true, num_args = 1.., value_name = "COMMAND", allow_hyphen_values = true)]
        command: Vec<String>,
    },

    #[clap(about = "Show manifest of all repos (commit hashes)")]
    Manifest,
}

#[derive(Parser, Clone)]
enum ToolKind {
    GenUuids {
        #[clap(short = 'n', long_help = "Number of uuids", default_value = "1000000")]
        num: usize,

        #[clap(short = 'f', long_help = "File output", default_value = "uuids.data")]
        file: String,
    },
    DescribeStack {
        #[clap(
            long_help = "CloudFormation stack name",
            default_value = "FractalbitsVpcStack"
        )]
        stack_name: String,
    },
    DumpVgConfig {
        #[clap(
            long,
            long_help = "Use localhost DynamoDB instead of AWS (for local development)"
        )]
        localdev: bool,
    },
}

#[tokio::main]
#[cmd_lib::main]
async fn main() -> CmdResult {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_target(false)
        .init();
    rlimit::increase_nofile_limit(1000000).unwrap();

    match Cmd::parse() {
        Cmd::Build { command, release } => match command {
            Some(build_cmd) => match build_cmd {
                BuildCommand::All => cmd_build::build_all(release)?,
                BuildCommand::Zig { command } => match command {
                    Some(ZigCommand::Test) => {
                        cmd_precheckin::run_zig_unit_tests(InitConfig::default())?
                    }
                    None => {
                        let build_mode = cmd_build::build_mode(release);
                        cmd_build::build_zig_servers(build_mode)?;
                    }
                },
                BuildCommand::Rust => {
                    let build_mode = cmd_build::build_mode(release);
                    cmd_build::build_rust_servers(build_mode)?;
                }
                BuildCommand::Prebuilt => cmd_build::build_prebuilt(release)?,
            },
            None => {
                // Default to building all components
                cmd_build::build_all(release)?;
            }
        },
        Cmd::Precheckin {
            s3_api_only,
            zig_unit_tests_only,
            debug_api_server,
            with_fractal_art_tests,
            with_https,
            data_blob_storage,
            docker,
        } => {
            let init_config = InitConfig {
                with_https,
                data_blob_storage,
                ..Default::default()
            };

            cmd_precheckin::run_cmd_precheckin(
                init_config,
                s3_api_only,
                zig_unit_tests_only,
                debug_api_server,
                with_fractal_art_tests,
                docker,
            )?;
        }
        Cmd::Nightly => cmd_nightly::run_cmd_nightly()?,
        Cmd::Bench {
            service,
            workload,
            with_flame_graph,
            keys_limit,
        } => {
            let mut service_name = ServiceName::All;
            cmd_bench::prepare_bench(with_flame_graph)?;
            cmd_bench::run_cmd_bench(
                service,
                workload,
                with_flame_graph,
                keys_limit,
                &mut service_name,
            )
            .inspect_err(|_| {
                cmd_service::stop_service(service_name).unwrap();
            })?;
        }
        Cmd::Service(service_cmd) => match service_cmd {
            ServiceCommand::Init {
                service,
                release,
                for_gui,
                data_blob_storage,
                with_https,
                bss_count,
                nss_disable_restart_limit,
                rss_backend,
                journal_type,
            } => {
                if bss_count != 1 && bss_count != 6 {
                    cmd_die!("bss_count must be either 1 or 6, got $bss_count");
                }
                let init_config = InitConfig {
                    for_gui,
                    data_blob_storage,
                    with_https,
                    bss_count,
                    nss_disable_restart_limit,
                    rss_backend,
                    journal_type,
                };
                cmd_service::init_service(service, cmd_build::build_mode(release), init_config)?;
            }
            ServiceCommand::Stop { service } => {
                cmd_service::stop_service(service)?;
            }
            ServiceCommand::Start { service } => {
                cmd_service::start_service(service)?;
            }
            ServiceCommand::Restart { service } => {
                cmd_service::stop_service(service)?;
                cmd_service::start_service(service)?;
            }
            ServiceCommand::Status { service } => {
                cmd_service::show_service_status(service)?;
            }
        },
        Cmd::Tools(tool_kind) => cmd_tool::run_cmd_tool(tool_kind)?,
        Cmd::Deploy(deploy_cmd) => match deploy_cmd {
            DeployCommand::Build {
                target,
                release,
                zig_extra_build,
                api_server_build_env,
            } => cmd_deploy::build(target, release, &zig_extra_build, &api_server_build_env)?,
            DeployCommand::Upload { deploy_target } => cmd_deploy::upload(deploy_target)?,
            DeployCommand::CreateVpc {
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
                ssm_bootstrap,
                journal_type,
                watch_bootstrap,
            } => cmd_deploy::create_vpc(cmd_deploy::VpcConfig {
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
                ssm_bootstrap,
                journal_type,
                watch_bootstrap,
            })?,
            DeployCommand::DestroyVpc => cmd_deploy::destroy_vpc()?,
            DeployCommand::BootstrapProgress { deploy_target } => {
                cmd_deploy::bootstrap::show_progress(deploy_target)?
            }
            DeployCommand::CreateCluster {
                config,
                bootstrap_s3_url,
                watch_bootstrap,
            } => cmd_deploy::create_cluster(&config, &bootstrap_s3_url, watch_bootstrap)?,
        },
        Cmd::RunTests { test_type } => {
            let test_type = test_type.unwrap_or(TestType::All);
            cmd_run_tests::run_tests(test_type).await?
        }
        Cmd::Repo(repo_cmd) => cmd_repo::run_cmd_repo(repo_cmd)?,
        Cmd::Docker(docker_cmd) => cmd_docker::run_cmd_docker(docker_cmd)?,
        Cmd::Prebuilt(prebuilt_cmd) => cmd_prebuilt::run_cmd_prebuilt(prebuilt_cmd)?,
    }
    Ok(())
}
