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

#### Optional Tools

- [aws ssm (session manager) plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/install-plugin-linux-overview.html) for login from terminal to manage ec2 instances after deployment
- [Nix](https://nixos.org/): recommended for a consistent development environment

### Codebase Organization

```
fractalbits-main/
├── core/                   # Zig data plane components (to be open)
│   ├── bss_server/         # Blob Storage Server
│   ├── nss_server/         # Namespace Service Server
│   └── lib/                # Shared Zig libraries
├── crates/                 # Rust workspace
│   ├── api_server/         # S3 API frontend
│   ├── root_server/        # Cluster coordination (to be open)
│   ├── ha/                 # High availability (to be open)
│   └── common/             # Shared libraries
│       ├── aws_signature/  # AWS SigV4 implementation
│       ├── rpc_clients/    # Client libraries
│       └── metrics_wrapper/# Observability
├── vpc/                    # AWS CDK deployment
└── xtask/                  # Rust for any tasks (Build, Service mgmt, Test ...)

```

### Development Workflow

1. Create a feature branch from `main`
2. Make changes and ensure code quality:
   - Run `cargo fmt` and `zig fmt`
   - Run `cargo clippy` for Rust linting
   - Add tests for new functionality
3. Run `just precheckin` to validate
4. Commit changes with descriptive messages
5. Submit a pull request

**Commit Message Guidelines:**
- Use imperative mood ("Add feature" not "Added feature")
- Keep first line under 72 characters
- Wrap body at 72 characters
- Reference issues when applicable

## Contributing

We welcome contributions from the community! Here's how you can help:

### Ways to Contribute

- 🐛 **Report bugs**: Open an issue with reproduction steps
- 💡 **Suggest features**: Propose new S3 API operations or optimizations
- 📝 **Improve documentation**: Help make our docs clearer
- 🔧 **Submit code**: Fix bugs or implement features
- 🧪 **Add tests**: Improve test coverage
- 📊 **Performance testing**: Run benchmarks and share results

### Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/fractalbits-labs/fractalbits-main.git`
3. Create a feature branch: `git checkout -b feature/my-feature`
4. Make your changes following our code style
5. Run tests: `just precheckin`
6. Commit and push to your fork
7. Open a Pull Request

### Code Review Process

All submissions require review. We'll:
- Check code quality and style
- Verify tests pass
- Assess performance impact
- Ensure documentation is updated

**Note**: Performance is critical to FractalBits. When making changes, always evaluate the performance impact. Our product's success depends on maintaining exceptional performance.
