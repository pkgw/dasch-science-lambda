# Lambda Functions for the DASCH Data Access

This repository defines the cloud-based DASCH science data access APIs. They are
implemented as [AWS Lambdas] in [Rust].

[AWS Lambdas]: https://aws.amazon.com/lambda/
[Rust]: https://rust-lang.org/

The code is built with a standard Rust `cargo build` command. This creates two
nearly-identical executables, `dasch-science-lambda-oneshot` and
`dasch-science-lambda-proxyevent`. The first is useful for local testing. The
second supports the [AWS API Gateway proxy event][proxy] protocol, which is what
is used in the deployed DASCH systems.

[proxy]: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html

The main APIs are:

- `src/cutout.rs` extracts cutout FITS images from the whole-plate mosaics
- `src/lightcurve.rs` retrieves a lightcurve for a specific source
- `src/platephot.rs` retrieves photometry for a subset of a single plate
- `src/mosaics.rs` retrieves information needed to assemble a "value-added mosaic FITS"
- `src/presign.rs` generates presigned S3 links for low-level data access
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
to it. Run with something like

```sh
cargo run --bin dasch-science-lambda-oneshot queryexps '{"ra_deg":0,"dec_deg":0}'
```


## Deployment

Deployment is automated through GitLab's CI infrastructure. Updates to the `dev`
branch lead to the `daschscience.zip` deployment package being updated on S3;
updates to `main` update the production version.

After the deployment package is updated, the API Gateway must be redeployed, which
is accomplished by triggering one of the apply pipelines of the [`aws_neo4j`]
repository.

[`aws_neo4j`]:  https://gitlab.com/HarvardRC/rse/cfa-dasch/infra/applications/aws_neo4j/
