build *args:
  cargo xtask build {{args}}

service *args:
  cargo xtask service {{args}}

precheckin *args:
  cargo xtask precheckin {{args}}

nightly *args:
  cargo xtask nightly {{args}}

run-tests *args:
  cargo xtask run-tests {{args}}

deploy *args:
  cargo xtask deploy {{args}}

prebuilt *args:
  cargo xtask prebuilt {{args}}

describe-stack *args:
  cargo xtask tools describe-stack {{args}}

dump-vg-config *args:
  cargo xtask tools dump-vg-config {{args}}

source-file *args:
  cargo xtask tools source-file {{args}}

repo *args:
  cargo xtask repo {{args}}

git *args:
  cargo xtask repo foreach git {{args}}

docker *args:
  cargo xtask docker {{args}}

# Run any xtask command with coverage instrumentation (resets previous data)
# Examples:
#   just coverage precheckin --docker=excluded
#   just coverage run-tests fs-server
#   just coverage precheckin --s3-api-only
coverage +args:
  #!/usr/bin/env bash
  set -euo pipefail
  source <(cargo llvm-cov show-env --sh --no-cfg-coverage)
  cargo llvm-cov clean --workspace
  cargo xtask {{args}}
  cargo llvm-cov report --html --ignore-filename-regex 'xtask/.*'
  cargo llvm-cov report --lcov --ignore-filename-regex 'xtask/.*' --output-path target/llvm-cov/lcov.info
  echo "HTML report: target/llvm-cov/html/index.html"
  echo "LCOV report: target/llvm-cov/lcov.info"

# Accumulate coverage from an xtask command (does not reset previous data)
# Run "just coverage ..." first, then "just coverage-add ..." to combine
# Example:
#   just coverage precheckin --docker=excluded
#   just coverage-add run-tests fs-server
#   just coverage-report
coverage-add +args:
  #!/usr/bin/env bash
  set -euo pipefail
  source <(cargo llvm-cov show-env --sh --no-cfg-coverage)
  cargo xtask {{args}}

# Generate coverage report from accumulated data
coverage-report:
  #!/usr/bin/env bash
  set -euo pipefail
  source <(cargo llvm-cov show-env --sh --no-cfg-coverage)
  cargo llvm-cov report --html --ignore-filename-regex 'xtask/.*'
  cargo llvm-cov report --lcov --ignore-filename-regex 'xtask/.*' --output-path target/llvm-cov/lcov.info
  echo "HTML report: target/llvm-cov/html/index.html"
  echo "LCOV report: target/llvm-cov/lcov.info"

# Quick unit-test-only coverage (no services needed)
coverage-unit:
  cargo llvm-cov --no-cfg-coverage --workspace --exclude api_server --exclude fs_server --ignore-filename-regex 'xtask/.*' --html
