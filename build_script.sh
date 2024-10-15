#! /bin/bash

set -xeuo pipefail
cargo build --release --target-dir=target/host
mkdir -p stage
cp target/host/release/dasch-science-lambda-bare stage/
