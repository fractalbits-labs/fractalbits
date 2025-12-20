use crate::*;
use std::fs;
use std::io;
use std::io::Error;
use std::path::Path;
use std::sync::LazyLock;
use strum::{AsRefStr, EnumString};
use zip::ZipArchive;

pub static BUILD_ENVS: LazyLock<Vec<String>> =
    LazyLock::new(|| build_envs().expect("failed to initialize BUILD_ENVS"));

pub fn get_build_envs() -> &'static Vec<String> {
    &BUILD_ENVS
}

#[derive(Copy, Clone, AsRefStr, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum BuildMode {
    Debug,
    Release,
}

pub fn build_mode(release: bool) -> BuildMode {
    match release {
        true => BuildMode::Release,
        false => BuildMode::Debug,
    }
}

pub fn build_envs() -> Result<Vec<String>, Error> {
    let timestamp = run_fun!(date "+%s")?;
    let main_info = get_repo_info(".")?;

    let mut envs = vec![
        format!("MAIN_BUILD_INFO={main_info}"),
        format!("BUILD_TIMESTAMP={timestamp}"),
    ];

    if Path::new(ZIG_REPO_PATH).exists() {
        let core_info = get_repo_info(ZIG_REPO_PATH)?;
        envs.push(format!("CORE_BUILD_INFO={core_info}"));
    }

    if Path::new("crates/ha").exists() {
        let ha_info = get_repo_info("crates/ha")?;
        envs.push(format!("HA_BUILD_INFO={ha_info}"));
    }

    if Path::new("crates/root_server").exists() {
        let rs_info = get_repo_info("crates/root_server")?;
        envs.push(format!("ROOT_SERVER_BUILD_INFO={rs_info}"));
    }

    Ok(envs)
}

fn get_repo_info(repo_path: &str) -> FunResult {
    if repo_path == "." || repo_path.is_empty() {
        let git_branch = run_fun!(git branch --show-current)?;
        let git_rev = run_fun!(git rev-parse --short HEAD)?;
        let dirty = if run_cmd!(git diff-index --quiet HEAD).is_ok() {
            ""
        } else {
            "+"
        };
        return Ok(format!("main:{git_branch}-{git_rev}{dirty}"));
    }

    if !Path::new(repo_path).exists() {
        return Ok(String::new());
    }

    let git_branch = run_fun!(cd $repo_path; git branch --show-current)?;
    let git_rev = run_fun!(cd $repo_path; git rev-parse --short HEAD)?;
    let dirty = if run_cmd!(cd $repo_path; git diff-index --quiet HEAD).is_ok() {
        ""
    } else {
        "+"
    };

    let repo_name = Path::new(repo_path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(repo_path);

    Ok(format!("{repo_name}:{git_branch}-{git_rev}{dirty}"))
}

pub fn build_bench_rpc() -> CmdResult {
    let build_envs = get_build_envs();
    run_cmd! {
        info "Building benchmark tool `rewrk_rpc` ...";
        cd crates/bench_rpc;
        $[build_envs] cargo build --release;
    }
}

pub fn build_zig_servers(mode: BuildMode) -> CmdResult {
    if !Path::new(ZIG_REPO_PATH).exists() {
        return Ok(());
    }

    let build_envs = get_build_envs();
    let opts = match mode {
        BuildMode::Debug => "",
        BuildMode::Release => "--release=safe",
    };
    run_cmd! {
        info "Building zig-based servers ...";
        cd $ZIG_REPO_PATH;
        $[build_envs] zig build -p ../$ZIG_DEBUG_OUT $opts 2>&1;
        info "Building bss and nss server done";
    }
}

pub fn build_rust_servers(mode: BuildMode) -> CmdResult {
    ensure_protoc()?;

    let build_envs = get_build_envs();
    match mode {
        BuildMode::Debug => {
            run_cmd! {
                info "Building rust-based servers in debug mode ...";
                $[build_envs] cargo build --workspace
                    --exclude fractalbits-bootstrap
                    --exclude rewrk*;
            }
        }
        BuildMode::Release => {
            run_cmd! {
                info "Building rust-based servers in release mode ...";
                $[build_envs] cargo build --workspace
                    --exclude container-all-in-one
                    --release;
            }
        }
    }
}

fn extract_zip(source: &str, dest: &str) -> io::Result<()> {
    let file = fs::File::open(source)?;
    let mut archive = ZipArchive::new(file)?;

    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let outpath = Path::new(dest).join(file.mangled_name());

        if file.is_dir() {
            fs::create_dir_all(&outpath)?;
        } else {
            if let Some(parent) = outpath.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut outfile = fs::File::create(&outpath)?;
            io::copy(&mut file, &mut outfile)?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if let Some(mode) = file.unix_mode() {
                    fs::set_permissions(&outpath, fs::Permissions::from_mode(mode))?;
                }
            }
        }
    }

    Ok(())
}

fn ensure_protoc() -> CmdResult {
    let protoc_dir = "third_party/protoc";
    let protoc_path = format!("{protoc_dir}/bin/protoc");

    if run_cmd!(bash -c "command -v protoc" &>/dev/null).is_ok() || Path::new(&protoc_path).exists()
    {
        return Ok(());
    }

    let version = "33.1";
    let base_url = "https://github.com/protocolbuffers/protobuf/releases/download";
    let file_name = format!("protoc-{version}-linux-x86_64.zip");
    let download_url = format!("{base_url}/v{version}/{file_name}");
    let zip_path = format!("third_party/{file_name}");

    info!("Downloading protoc binary since command not found");
    run_cmd!(mkdir -p $protoc_dir)?;

    // Try download with retry
    let mut download_success = false;
    for attempt in 1..=2 {
        if run_cmd!(curl -qL -o $zip_path $download_url 1>"/dev/null" 2>&1).is_ok() {
            download_success = true;
            break;
        }
        if attempt == 1 {
            info!("Download failed, retrying...");
        }
    }

    if !download_success {
        return Err(std::io::Error::other(
            "Failed to download protoc after 2 attempts",
        ));
    }

    extract_zip(&zip_path, protoc_dir)?;

    Ok(())
}

pub fn build_ui(region: &str) -> CmdResult {
    if !Path::new(UI_REPO_PATH).exists() {
        return Ok(());
    }

    run_cmd! {
        info "Building ui ...";
        cd $UI_REPO_PATH;
        npm install;
        VITE_AWS_REGION=$region npm run build;
    }
}

pub fn build_all(release: bool) -> CmdResult {
    let build_mode = build_mode(release);
    build_rust_servers(build_mode)?;
    build_zig_servers(build_mode)?;
    if release {
        build_bench_rpc()?;
    }
    build_ui(crate::UI_DEFAULT_REGION)?;
    Ok(())
}

pub fn build_prebuilt(_release: bool) -> CmdResult {
    let build_target = "x86_64-unknown-linux-gnu";
    let build_dir = format!("target/{build_target}/release");
    let build_envs = get_build_envs();

    if !Path::new(&format!("{ZIG_REPO_PATH}/.git/")).exists() {
        warn!("No core repo found, skip building prebuilt");
        return Ok(());
    }

    run_cmd! {
        info "Building Zig binaries for x86_64_v3 (release mode)...";
        cd $ZIG_REPO_PATH;
        $[build_envs] zig build -p ../$build_dir/zig-out
            -Doptimize=ReleaseSafe
            -Dtarget=x86_64-linux-gnu
            -Dcpu=x86_64_v3
            -Dfor_prebuilt=true 2>&1;
        info "Zig build complete";
    }?;

    run_cmd! {
        info "Building Rust binaries for x86_64_v3 (size-optimized release mode with zigbuild)...";
        RUSTFLAGS="-C target-cpu=x86-64-v3 -C opt-level=z -C codegen-units=1 -C strip=symbols"
        $[build_envs] cargo zigbuild --release --target $build_target
            --workspace --exclude fractalbits-bootstrap --exclude rewrk*;
    }?;

    info!("Copying binaries to prebuilt directory...");
    run_cmd!(mkdir -p prebuilt)?;
    for bin in [
        "nss_role_agent",
        "root_server",
        "rss_admin",
        "zig-out/bin/bss_server",
        "zig-out/bin/nss_server",
    ] {
        run_cmd!(cp -f $build_dir/$bin prebuilt/)?;
    }

    run_cmd! {
        info "Stripping debug symbols...";
        find prebuilt -maxdepth 1 -type f -executable ! -name "*.d"
            -exec strip --strip-all "{}" +;
    }?;

    info!("Prebuilt binaries are ready");
    Ok(())
}
