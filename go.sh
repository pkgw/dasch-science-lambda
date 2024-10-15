#! /bin/bash

if [ -z "$AWS_REGION" -o -z "$AWS_ACCESS_KEY_ID" -o -z "$AWS_SECRET_ACCESS_KEY" ] ; then
  echo >&2 "fatal: need to set \$AWS_REGION, \$AWS_ACCESS_KEY_ID, and/or \$AWS_SECRET_ACCESS_KEY"
  exit 1
fi

func="$1"
shift

if [ -z "$func" ] ; then
  echo >&2 "fatal: need to specify a function: cutout, querycat, queryexps"
  exit 1
fi

set -xeuo pipefail

cargo check
docker run --rm \
  -v $(pwd):/app:rw,z \
  -v $(pwd)/target/host_registry:/usr/local/cargo/registry:rw,z \
  dasch-science-lambda-builder:latest
docker build -t dasch-science-lambda:latest -f Dockerfile.lambda .

export DASCH_LOCALTEST_ARN="$func"

exec docker run --rm \
  -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  -e DASCH_LOCALTEST_ARN \
  -p 9000:8080 \
  dasch-science-lambda:latest
