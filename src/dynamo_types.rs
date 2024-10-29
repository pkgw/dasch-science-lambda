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

/// Types for querying the reference catalog tables for the purposes of
/// lightcurve generation.
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

/// Types for querying the reference catalog tables for the querycat API call.
pub mod refcat_querycat {
    use super::*;
    use crate::{gscbin::D2R, refnums::refnum_to_text};

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct RefcatItem {
        pub gsc_bin_index: u32,
        pub ref_number: u64,
        pub ra: f64,
        pub dec: f64,
        pub stdmag: f32,
        pub color: f32,
        #[serde(rename = "raPM")]
        pub ra_pm: f32,
        #[serde(rename = "decPM")]
        pub dec_pm: f32,
        #[serde(rename = "raSigmaPM")]
        pub ra_sigma_pm: f32,
        #[serde(rename = "decSigmaPM")]
        pub dec_sigma_pm: f32,
        pub class: u8,
        pub v_flag: u8,
        pub mag_flag: u8,

        /// Extra per-source photometry information
        pub phot: Option<RefcatPhot>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct RefcatPhot {
        pub count: u32,
    }

    impl RefcatItem {
        /// Get a CSV header row appropriate for CSV-format display of these items
        ///
        /// This must be kept in sync with `as_csv_row()`, of course.
        pub fn csv_header() -> String {
            "\
            ref_text,\
            ref_number,\
            gsc_bin_index,\
            ra_deg,\
            dec_deg,\
            pos_epoch,\
            stdmag,\
            color,\
            pm_ra_masyr,\
            u_pm_ra_masyr,\
            pm_dec_masyr,\
            u_pm_dec_masyr,\
            class,\
            v_flag,\
            mag_flag,\
            num_matches,\
            dra_asec,\
            ddec_asec\
            "
            .to_owned()
        }

        /// Express this source as a CSV row.
        pub fn as_csv_row(&self, query_ra: f64, query_dec: f64) -> String {
            let mut delta_ra = query_ra - self.ra;

            if delta_ra < -180. {
                delta_ra += 360.;
            } else if delta_ra > 180. {
                delta_ra -= 360.;
            }

            let factor = (D2R * 0.5 * (self.dec + query_dec)).cos();
            let dra = 3600. * factor * delta_ra;
            let ddec = 3600. * (query_dec - self.dec);

            let mut cells = Vec::new();

            cells.push(refnum_to_text(self.ref_number));
            cells.push(self.ref_number.to_string());
            cells.push(self.gsc_bin_index.to_string());
            cells.push(self.ra.to_string());
            cells.push(self.dec.to_string());
            cells.push("2000.0".to_string());
            cells.push(self.stdmag.to_string());
            cells.push(self.color.to_string());
            cells.push(self.ra_pm.to_string());
            cells.push(self.ra_sigma_pm.to_string());
            cells.push(self.dec_pm.to_string());
            cells.push(self.dec_sigma_pm.to_string());
            cells.push(self.class.to_string());
            cells.push(self.v_flag.to_string());
            cells.push(self.mag_flag.to_string());
            cells.push(
                self.phot
                    .as_ref()
                    .map(|p| p.count.to_string())
                    .unwrap_or_default(),
            );
            cells.push(dra.to_string());
            cells.push(ddec.to_string());

            cells.join(",")
        }
    }
}
