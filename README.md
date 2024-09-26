# Lambda Functions for the DASCH Data Access

Sigh, this is silly.

Build the builder image:

```
docker build -t dasch-science-lambda-builder:latest -f Dockerfile.build .
```

Use the builder image to build the program:

```
docker run --rm -v $(pwd):/app:rw,z -v $(pwd)/target/host_registry:/usr/local/cargo/registry:rw,z dasch-science-lambda-builder:latest
```

AFAICT think that we need a separate builder image to be able to cache all of
the intermediates such that we can rebuild quickly.

Build the Lambda container:

```
docker build -t dasch-science-lambda:latest -f Dockerfile.lambda .
```

Run it locally:

```
docker run --rm -p 9000:8080 dasch-science-lambda:latest
```
