//! "Proxy event" version of the DASCH science Lambda implementations.
//!
//! This executable defines a server that expects to be interacted with
//! according to AWS API Gateway's "proxy event" protocol. This adds an
//! additional layer of complexity beyond simple JSON-in, JSON-out. The "bare"
//! version of the server is simpler and is more useful for local testing.

use lambda_http::{run, service_fn, Error, Request, RequestExt, RequestPayloadExt};
use serde_json::Value;

use dasch_science_lambda::Services;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let svcs = Services::init().await?;
    let ref_svcs = &svcs;

    run(service_fn(|req: Request| async move {
        let context = req.lambda_context();
        let payload: Option<Value> = req.payload()?;
        ref_svcs
            .dispatch(context.invoked_function_arn, payload)
            .await
    }))
    .await?;
    Ok(())
}
