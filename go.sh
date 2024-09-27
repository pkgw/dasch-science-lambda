#! /bin/bash

set -xeuo pipefail

cargo check
docker run --rm \
  -v $(pwd):/app:rw,z \
  -v $(pwd)/target/host_registry:/usr/local/cargo/registry:rw,z \
  dasch-science-lambda-builder:latest
docker build -t dasch-science-lambda:latest -f Dockerfile.lambda .
exec docker run --rm \
  -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  -p 9000:8080 \
  dasch-science-lambda:latest
