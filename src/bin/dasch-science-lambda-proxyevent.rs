//! "Proxy event" version of the DASCH science Lambda implementations.
//!
//! This executable defines a server that expects to be interacted with
//! according to AWS API Gateway's "proxy event" protocol. This adds an
//! additional layer of complexity beyond simple JSON-in, JSON-out. The
//! "oneshot" version of the server is more useful for local testing.

use lambda_http::{run, service_fn, Error, Request, RequestExt, RequestPayloadExt};
use serde_json::{json, Value};

use dasch_science_lambda::Services;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let svcs = Services::init().await?;
    let ref_svcs = &svcs;

    run(service_fn(|req: Request| async move {
        let context = req.lambda_context();
        let mut payload: Option<Value> = req.payload()?;

        // This is a hack to give our implementations access to API Gateway
        // "path parameters", which are delivered separately from the payload.
        // We could do something similar to expose query string parameters and
        // "stage variables".
        //
        // If the request payload body is not map-like, we won't be able to
        // insert the path parameters. This will almost surely cause an API
        // error since the API presumably depends on those parameters. On the
        // other hand, we make sure to be OK with non-map-like bodies if there
        // are no path parameters. (I believe that `path_parameters_ref()` only
        // returns Some if there are any path parameters to transfer.)

        if let Some(vars) = req.path_parameters_ref() {
            if let Some(pmap) = payload.get_or_insert_with(|| json!({})).as_object_mut() {
                for (name, value) in vars.iter() {
                    pmap.insert(name.to_owned(), Value::String(value.to_owned()));
                }
            }
        }

        ref_svcs
            .dispatch(context.invoked_function_arn, payload)
            .await
    }))
    .await?;
    Ok(())
}
