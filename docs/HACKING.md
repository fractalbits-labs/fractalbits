# FractalBits Development Guide

This guide covers the prerequisites and setup required for developing FractalBits.

## Prerequisites

### System Requirements

- **Linux** kernel 5.19+ (for io_uring support), ubuntu 24.04+ recommended
- [**Rust**](https://rust-lang.org/learn/get-started/) tool chain 1.88+
- **Disk Space**: At least 20GB+ free space for development (data and builds)
- **Memory**: 8GB+ recommended, or it might trigger OOM killer, depending on your OS configuration

### Required Dependencies

#### Basic Build Tools

Install build tools, git, OpenSSL development libraries, Protocol Buffers compiler, and Java runtime. Java is required to run DynamoDB Local, which is used by the RSS (Root Service Server) for cluster coordination during local development. We also install [`just`](https://github.com/casey/just), a command runner that provides convenient shortcuts for common development tasks (e.g., `just build` instead of `cargo xtask build`).

```bash
# Debian/Ubuntu
sudo apt-get update && sudo apt-get install git just build-essential moreutils libssl-dev pkg-config default-jre protobuf-compiler

# Fedora
sudo dnf install git just gcc moreutils openssl-devel java-latest-openjdk protobuf-compiler

# Arch Linux
sudo pacman -S --needed base-devel git just moreutils openssl pkg-config jre-openjdk protobuf
```

#### AWS CLI

AWS CLI is required to initialize DynamoDB tables during local service setup (`just service init`).

```bash
# Install via pip
pip install awscli

# Or download directly (recommended)
# See: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

# Debian/Ubuntu (via snap)
sudo snap install aws-cli --classic

# Fedora
sudo dnf install awscli2

# Arch Linux
sudo pacman -S aws-cli-v2

# Verify installation
aws --version
```

#### Zig and npm (Deployment Only)

These tools are only required if you plan to deploy FractalBits to AWS. They are not needed for local development and testing.

Zig is required for cross-compilation (cargo-zigbuild), and npm is needed for CDK deployment builds.

```bash
# Debian/Ubuntu
sudo snap install zig --classic --beta # Or download directly from https://ziglang.org/download/
sudo apt install npm

# Fedora
sudo dnf install zig npm

# Arch Linux
sudo pacman -S zig npm

# Verify installation
zig version
npm --version
```

#### Docker (Optional)

Docker is required for building and testing container images (`just docker build`, `just precheckin --docker=only`).

```bash
# Debian/Ubuntu
sudo apt install docker.io docker-buildx

# Fedora
sudo dnf install docker docker-buildx

# Arch Linux
sudo pacman -S docker docker-buildx

# Add your user to the docker group (requires logout/login)
sudo usermod -aG docker $USER

# Verify installation
docker --version
docker buildx version
```

#### NFS Client (Optional)

Required for running NFS integration tests (`just run-tests fs-server --nfs`).

```bash
# Debian/Ubuntu
sudo apt-get install nfs-common

# Fedora
sudo dnf install nfs-utils

# Arch Linux
sudo pacman -S nfs-utils
```

#### Optional Tools

- [aws ssm (session manager) plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/install-plugin-linux-overview.html) for login from terminal to manage ec2 instances after deployment
- [Nix](https://nixos.org/): recommended for a consistent development environment

### Development Workflow

1. Create a feature branch from `main`
2. Make changes and ensure code quality:
   - Run `cargo fmt` and `zig fmt`
   - Run `cargo clippy` for Rust linting
   - Add tests for new functionality
3. Run `just precheckin` to validate
4. Commit changes with descriptive messages
5. Submit a pull request

**Note**: Performance is critical to FractalBits. When making changes, always evaluate the performance impact. Our product's success depends on maintaining exceptional performance.
