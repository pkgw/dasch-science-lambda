# Lambda Functions for the DASCH Data Access

This repository defines the cloud-based DASCH science data access APIs. They are
implemented as [AWS Lambdas] in [Rust].

[AWS Lambdas]: https://aws.amazon.com/lambda/
[Rust]: https://rust-lang.org/

The code is built with a standard Rust `cargo build` command. This creates three
nearly-identical executables, `dasch-science-lambda-oneshot`,
`dasch-science-lambda-bare` and `dasch-science-lambda-proxyevent`. The first two
are useful for local testing. The third supports the [AWS API Gateway proxy
event][proxy] protocol, which is what is used in the deployed DASCH systems.

[proxy]: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html

The main APIs are:

- `src/cutout.rs` extracts cutout FITS images from the whole-plate mosaics
- `src/querycat.rs` queries one of the “reference catalogs” for sources
- `src/queryexps.rs` queries for plate exposures overlapping a specified sky
  coordinate. (Plates may have multiple exposures at different sky positions, so
  one exposure may overlap the coordinate while another does not.)


## Local Testing

Local testing of the service requires direct access to DASCH cloud resources
such as our [DynamoDB] and [S3] assets. Therefore, it's only possible for people
with sufficient permissions on the DASCH cloud infrastructure.

[DynamoDB]: https://aws.amazon.com/dynamodb/
[S3]: https://aws.amazon.com/s3/

If that’s you, the `oneshot` executable performs one API request, taking the API
name (the Lambda function ARN, in the AWS context) and a JSON payload as
command-line arguments. This is the easiest to run and you can attach a debugger
to it.

To run the `bare` API server, first build the builder image:

```
docker build -t dasch-science-lambda-builder:latest -f Dockerfile.build .
```

Then, to start a server for testing a specific function, use:

```
./go.sh <FUNCTION>  # <FUNCTION> is one of `cutout`, `querycat`, `queryexps`
```

Make requests to the server with commands of the following form:

```
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"ra_deg":0,"dec_deg":0}'
```


## Deployment

Deployment is automated through GitLab's CI infrastructure. Updates to the `dev`
branch lead to the `daschscience.zip` deployment package being updated on S3;
updates to `main` update the production version.

After the deployment package is updated, the API Gateway must be redeployed, which
is accomplished by triggering one of the apply pipelines of the [`aws_neo4j`]
repository.

[`aws_neo4j`]:  https://gitlab.com/HarvardRC/rse/cfa-dasch/infra/applications/aws_neo4j/


## Unmanaged resources to close out

- `dasch_dev_refcat_apass` DynamoDB table
- `dasch-dr7/dev` ECR private registry
- `dasch-dev-dr7-querycat` Lambda
- `dasch-dev-dr7-querycat-role-gw8usqt5` lambda exec role
