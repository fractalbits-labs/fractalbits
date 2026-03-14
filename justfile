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
  find . -name '*.profraw' -not -path './target/*' -delete 2>/dev/null || true
  cargo xtask {{args}}
  # Collect stray profraw files written by services/tests
  find . -name '*.profraw' -not -path './target/*' -exec mv {} target/ \; 2>/dev/null || true
  just coverage-report

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
  # Collect stray profraw files written by services/tests
  find . -name '*.profraw' -not -path './target/*' -exec mv {} target/ \; 2>/dev/null || true

# Generate coverage report from accumulated data.
# Uses llvm tools directly (instead of cargo llvm-cov report) to include
# fs_server from the isolated compio build that cargo llvm-cov can't discover.
coverage-report:
  #!/usr/bin/env bash
  set -euo pipefail
  source <(cargo llvm-cov show-env --sh --no-cfg-coverage)
  LLVM="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's|host: ||p')/bin"

  # Merge profraw files
  find target/ -maxdepth 1 -name '*.profraw' > target/profraw-list
  [[ -s target/profraw-list ]] || { echo "No profraw files found"; exit 1; }
  "$LLVM/llvm-profdata" merge -sparse -f target/profraw-list -o target/coverage.profdata

  # Find all instrumented ELF binaries (main build + compio build)
  OBJECTS=()
  for f in $(find target/debug/deps target/debug target/compio/debug \
      -maxdepth 1 -type f -executable 2>/dev/null); do
    file "$f" | grep -q 'ELF' && OBJECTS+=(-object "$f")
  done

  COMMON=(-instr-profile=target/coverage.profdata "${OBJECTS[@]}" \
    -ignore-filename-regex 'xtask/|/rustc/|/\.cargo/|/\.rustup/|/target/|infra/|bench_rpc/|container-all-in-one/|fractal-s3/')

  mkdir -p target/llvm-cov/html
  "$LLVM/llvm-cov" show  -format=html "${COMMON[@]}" -show-instantiations=false \
    -show-line-counts-or-regions -output-dir=target/llvm-cov/html
  "$LLVM/llvm-cov" export -format=lcov "${COMMON[@]}" > target/llvm-cov/lcov.info

  echo "HTML report: target/llvm-cov/html/index.html"
  echo "LCOV report: target/llvm-cov/lcov.info"

# Quick unit-test-only coverage (no services needed)
coverage-unit:
  cargo llvm-cov --no-cfg-coverage --workspace --exclude api_server --exclude fs_server --ignore-filename-regex 'xtask/.*' --html
