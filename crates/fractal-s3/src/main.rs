mod client;
mod commands;
mod s3_path;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "fractal-s3")]
#[command(about = "AWS S3 CLI replacement tool", long_about = None)]
struct Cli {
    #[arg(long, global = true, help = "Override S3 endpoint URL")]
    endpoint_url: Option<String>,

    #[arg(long, global = true, help = "Override region")]
    region: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Copy files to/from S3
    Cp {
        /// Source path (local or s3://bucket/key)
        src: String,
        /// Destination path (local or s3://bucket/key)
        dst: String,
        #[arg(short, long, help = "Copy recursively")]
        recursive: bool,
        #[arg(long, help = "Exclude files matching pattern")]
        exclude: Option<String>,
        #[arg(long, help = "Include files matching pattern")]
        include: Option<String>,
    },
    /// List buckets or objects
    Ls {
        /// S3 URI (optional, omit to list buckets)
        s3_uri: Option<String>,
        #[arg(long, help = "List recursively")]
        recursive: bool,
        #[arg(long, help = "Show sizes in human-readable format")]
        human_readable: bool,
    },
    /// Make bucket
    Mb {
        /// S3 bucket URI (s3://BUCKET)
        s3_uri: String,
    },
    /// Move files (supports atomic folder rename)
    Mv {
        /// Source path (local or s3://bucket/key)
        src: String,
        /// Destination path (local or s3://bucket/key)
        dst: String,
        #[arg(short, long, help = "Move recursively")]
        recursive: bool,
    },
    /// Remove bucket
    Rb {
        /// S3 bucket URI (s3://BUCKET)
        s3_uri: String,
        #[arg(long, help = "Remove non-empty bucket")]
        force: bool,
    },
    /// Remove objects
    Rm {
        /// S3 URI (s3://bucket/key)
        s3_uri: String,
        #[arg(short, long, help = "Remove recursively")]
        recursive: bool,
        #[arg(short, long, help = "Quiet mode (suppress output)")]
        quiet: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let client_config = client::S3ClientConfig {
        endpoint_url: cli.endpoint_url.clone(),
        region: cli.region.clone(),
    };

    let s3_client = client::create_s3_client(client_config.clone()).await?;

    match cli.command {
        Commands::Cp {
            src,
            dst,
            recursive,
            exclude,
            include,
        } => {
            commands::cp::execute(
                &s3_client,
                &client_config,
                &src,
                &dst,
                recursive,
                exclude.as_deref(),
                include.as_deref(),
            )
            .await?;
        }
        Commands::Ls {
            s3_uri,
            recursive,
            human_readable,
        } => {
            commands::ls::execute(&s3_client, s3_uri.as_deref(), recursive, human_readable).await?;
        }
        Commands::Mb { s3_uri } => {
            commands::mb::execute(&s3_client, &s3_uri).await?;
        }
        Commands::Mv {
            src,
            dst,
            recursive,
        } => {
            commands::mv::execute(&s3_client, &client_config, &src, &dst, recursive).await?;
        }
        Commands::Rb { s3_uri, force } => {
            commands::rb::execute(&s3_client, &s3_uri, force).await?;
        }
        Commands::Rm {
            s3_uri,
            recursive,
            quiet,
        } => {
            commands::rm::execute(&s3_client, &s3_uri, recursive, quiet).await?;
        }
    }

    Ok(())
}
