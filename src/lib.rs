//! The AWS/Lambda-powered DASCH data services
//!
//! This library crate implements the data services needed for the DASCH
//! Lambdas. This common codebase is then compiled into two executables:
//! `dasch-science-lambda-oneshot` and `dasch-science-lambda-proxyevent`. The
//! first is useful for local testing. The second has support for the more
//! complex AWS API Gateway "proxy event" framework that we use for our actual
//! cloud deployment.
//!
//! It was hard to find good examples of how a Rust Lambda implementation should
//! look. Here's one good one:
//!
//! <https://github.com/awslabs/aws-sdk-rust/tree/main/examples/cross_service/photo_asset_management>
//!
//! Streaming lambdas are more expensive than buffered lambdas, which have a 6
//! MB response limit. So we should buffer when possible.
//!
//! In my first implementation, it was only possible to emit strictly JSON
//! outputs, so several of the APIs return JSON when they would more naturally
//! return something like CSV. I have since figured out how to avoid that
//! limitation.

use lambda_runtime::{tracing, Error};
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;

mod cutout;
mod dynamo_types;
mod fitsfile;
mod gscbin;
pub mod http_types;
mod lightcurve;
mod mosaics;
mod photdata;
mod platephot;
mod presign;
mod querycat;
mod queryexps;
mod refnums;
mod s3buffer;
mod s3fits;
mod wcs;

/// I included a "dev" in this name even though in retrospect I wish I hadn't;
/// can't change it now.
pub const PLATES_TABLE_NAME: &str = "dasch-dev-dr7-plates";

pub const USER_BUCKET: &str = "dasch-prod-user";

pub const INFRA_BUCKET: &str = "dasch-prod-infra";

/// I included a "dev" in these names even though in retrospect I wish I hadn't;
/// can't change it now.
pub fn make_refcat_table_name(refcat: &str) -> String {
    format!("dasch-dev-dr7-refcat-{}", refcat)
}

pub struct Services {
    dc: aws_sdk_dynamodb::Client,
    s3c: aws_sdk_s3::Client,
    bin1: gscbin::GscBinning,
    bin2: gscbin::GscBinning,
    bin64: gscbin::GscBinning,
    presign_config: aws_sdk_s3::presigning::PresigningConfig,
}

pub fn simple_response<T: Serialize>(value: &T) -> Result<http_types::Response, Error> {
    let builder = http_types::ResponseBuilder::new();
    let text = serde_json::to_string(value)?;
    Ok(builder.body(http_types::Body::Text(text))?)
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
        let bin2 = gscbin::GscBinning::new2();
        let bin64 = gscbin::GscBinning::new64();
        let presign_config =
            aws_sdk_s3::presigning::PresigningConfig::expires_in(Duration::from_secs(900))?;

        Ok(Services {
            dc,
            s3c,
            bin1,
            bin2,
            bin64,
            presign_config,
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
    /// implementing and not have to check for every invocation. But in a small
    /// amount of testing I couldn't quite figure out how to do this. Nominally the
    /// `_HANDLER` environment variable should tell us what function we are, but
    /// with our deployment method, it's always set to `bootstrap`. This is almost
    /// surely all about my ignorance of how Lambda works.
    pub async fn dispatch(
        &self,
        mut arn: String,
        payload: Option<Value>,
    ) -> Result<http_types::Response, Error> {
        // Local testing environment?
        if arn.ends_with(":test_function") {
            arn = std::env::var("DASCH_LOCALTEST_ARN").unwrap();
        }

        if arn.ends_with("cutout") {
            Ok(cutout::handler(payload, &self.dc).await?)
        } else if arn.ends_with("lightcurve") {
            Ok(lightcurve::handler(payload, &self.dc, &self.s3c, &self.bin2).await?)
        } else if arn.ends_with("mosaic_package") {
            Ok(
                mosaics::handle_mosaic_package(payload, &self.dc, &self.s3c, &self.presign_config)
                    .await?,
            )
        } else if arn.ends_with("platephot") {
            Ok(platephot::handler(payload, &self.dc, &self.s3c, &self.bin64).await?)
        } else if arn.ends_with("presign_photcal_asdf") {
            Ok(presign::handle_photcal_asdf(payload, &self.s3c, &self.presign_config).await?)
        } else if arn.ends_with("querycat") {
            Ok(querycat::handler(payload, &self.dc, &self.bin64).await?)
        } else if arn.ends_with("queryexps") {
            Ok(queryexps::handler(payload, &self.dc, &self.s3c, &self.bin1, &self.bin2).await?)
        } else {
            Err(format!("unhandled function: {}", arn).into())
        }
    }
}
