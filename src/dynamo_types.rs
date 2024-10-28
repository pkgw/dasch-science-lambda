//! Type definitions for DynamoDB interactions
//!
//! These are meant to be used with `serde-dynamo` to interact with DynamoDB
//! records.
//!
//! The awkward thing is that due to the nature of a NoSQL database, and our
//! usage, we are always "projecting" the full item types to subsets containing
//! only the fields we need for a given operation. I'm not aware of a clean way
//! to do that with serde-dynamo (although, TBF, I haven't even looked). My best
//! idea is to define the projections in sub-modules here so that we can at
//! least keep the relevant code together.
//!
//! This is a work-in-progress; other modules define types for DynamoDB
//! interactions that we should migrate here.

use serde::Deserialize;

pub mod refcat_lightcurve {
    use super::*;

    /// `count`, `dec`, and `offset` are reserveds word (!), so we have to use
    /// expression attribute name placeholders here.
    pub const PROJECTION_EXPRESSION: &str = "ra,#DEC,phot.#COUNT,phot.#OFFSET";

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct RefcatItem {
        pub phot: Option<RefcatPhot>,
        pub ra: f64,
        pub dec: f64,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct RefcatPhot {
        pub count: u32,

        /// The largest individual phot files are around 400 MB, so offsets into
        /// them can fit into a u32.
        pub offset: u32,
    }
}
