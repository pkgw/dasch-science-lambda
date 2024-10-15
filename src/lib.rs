//! The AWS/Lambda-powered DASCH data services
//!
//! This library crate implements the data services needed for the DASCH
//! Lambdas. This common codebase is then compiled into two different
//! executables: `dasch-science-lambda-bare` and
//! `dasch-science-lambda-proxyevent`. The former is useful for local testing.
//! while the latter has support for the more complex AWS API Gateway "proxy
//! event" framework that we for our actual cloud deployment.
//!
//! It was hard to find good examples of how a Rust Lambda implementation should
//! look. Here's one good one:
//!
//! <https://github.com/awslabs/aws-sdk-rust/tree/main/examples/cross_service/photo_asset_management>
//!
//! Streaming lambdas are more expensive than buffered lambdas, which have a 6
//! MB response limit. So we should buffer when possible.
//!
//! Annoyingly, the buffered response mechanism can *only* output JSON, so we
//! can't emit CSV.

use lambda_runtime::{tracing, Error};
use serde_json::Value;

mod cutout;
mod fitsfile;
mod gscbin;
mod mosaics;
mod querycat;
mod queryexps;
mod refnums;
mod s3buffer;
mod s3fits;
mod wcs;

pub const ENVIRONMENT: &str = "dev";

pub struct Services {
    dc: aws_sdk_dynamodb::Client,
    s3c: aws_sdk_s3::Client,
    bin1: gscbin::GscBinning,
    bin64: gscbin::GscBinning,
}

impl Services {
    /// Create a state object for the DASCH science data Lambda services.
    pub async fn init() -> Result<Self, Error> {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_target(false) // don't print the module name
            .without_time() // don't print time (CloudWatch has it)
            .init();

        let config = aws_config::load_from_env().await;

        s3fits::register(config.clone());

        let dc = aws_sdk_dynamodb::Client::new(&config);
        let s3c = aws_sdk_s3::Client::new(&config);
        let bin1 = gscbin::GscBinning::new1();
        let bin64 = gscbin::GscBinning::new64();

        Ok(Services {
            dc,
            s3c,
            bin1,
            bin64,
        })
    }

    /// Handle an invocation of one of the DASCH science APIs.
    ///
    /// We *could* provide a separate deployment package for each different API, but
    /// it seems straightforward enough to bundle them all into one executable. We
    /// "know" which function is being invoked by looking at the suffix of the
    /// function ARN.
    ///
    /// Each Lambda server process is only responsible for executing a particular
    /// function, so in principle we ought to be able to know which function we're
    /// implementating and not have to check for every invocation. But in a small
    /// amount of testing I couldn't quite figure out how to do this. Nominally the
    /// `_HANDLER` environment variable should tell us what function we are, but
    /// with our deployment method, it's always set to `bootstrap`. This is almost
    /// surely all about my ignorance of how Lambda works.
    pub async fn dispatch(&self, mut arn: String, payload: Option<Value>) -> Result<Value, Error> {
        // Local testing environment?
        if arn.ends_with(":test_function") {
            arn = std::env::var("DASCH_LOCALTEST_ARN").unwrap();
        }

        if arn.ends_with("cutout") {
            Ok(cutout::handler(payload, &self.dc).await?)
        } else if arn.ends_with("querycat") {
            Ok(querycat::handler(payload, &self.dc, &self.bin64).await?)
        } else if arn.ends_with("queryexps") {
            Ok(queryexps::handler(payload, &self.dc, &self.s3c, &self.bin1).await?)
        } else {
            Err(format!("unhandled function: {}", arn).into())
        }
    }
}
