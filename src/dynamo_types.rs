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

/// Types for querying the plates table for the `mosaic_package` endpoint.
pub mod plates_mosaic_package {
    use serde::Serialize;

    use super::*;

    /// This tiny helper helps us to magically deserialize these types *from*
    /// DynamoDB with serde_dynamo, and serialize them *to* plain JSON using base64 encoding
    /// of the byte vectors.
    mod serb64 {
        use base64::engine::general_purpose::STANDARD;
        use serde::Serializer;

        pub fn serialize<S: Serializer>(value: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_str(&base64::display::Base64Display::new(value, &STANDARD))
        }
    }

    pub const PROJECTION_EXPRESSION: &str = "\
        astrometry.b01HeaderGz,\
        astrometry.exposures,\
        astrometry.resultId,\
        astrometry.rotationDelta,\
        mosaic.b01Height,\
        mosaic.b01OrigFileMD5,\
        mosaic.b01OrigFileSize,\
        mosaic.b01Width,\
        mosaic.b16OrigFileMD5,\
        mosaic.b16OrigFileSize,\
        mosaic.creationDate,\
        mosaic.legacyComment,\
        mosaic.legacyRotation,\
        mosaic.mosNum,\
        mosaic.scanNum,\
        mosaic.resultId,\
        mosaic.s3KeyTemplate,\
        photometry.medianColortermApass,\
        photometry.medianColortermAtlas,\
        photometry.nMagdepApass,\
        photometry.nMagdepAtlas,\
        photometry.nSolutionsApass,\
        photometry.nSolutionsAtlas,\
        photometry.resultIdApass,\
        photometry.resultIdAtlas,\
        plateNumber,\
        series";

    #[derive(Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PlatesResult {
        pub astrometry: Option<PlatesAstrometryResult>,
        pub mosaic: Option<PlatesMosaicResult>,
        pub photometry: Option<PlatesPhotometryData>,
        pub plate_number: usize,
        pub series: String,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PlatesAstrometryResult {
        // `default` because this should be Option<>, but not sure how to nest the custom deserializer
        #[serde(
            default,
            deserialize_with = "serde_bytes::deserialize",
            serialize_with = "serb64::serialize"
        )]
        pub b01_header_gz: Vec<u8>,

        #[serde(
            default,
            deserialize_with = "serde_bytes::deserialize",
            serialize_with = "serb64::serialize"
        )]
        pub result_id: Vec<u8>,

        pub rotation_delta: Option<isize>,
        pub exposures: Vec<Option<PlatesExposureResult>>,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PlatesExposureResult {
        pub center_source: Option<String>,
        pub date_acc_days: Option<f64>,
        pub date_source: Option<String>,
        pub dec_deg: Option<f64>,
        pub dur_min: Option<f64>,
        pub midpoint_date: Option<String>,
        pub number: i8,
        pub ra_deg: Option<f64>,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PlatesMosaicResult {
        pub b01_height: usize,
        #[serde(
            rename = "b01OrigFileMD5",
            deserialize_with = "serde_bytes::deserialize",
            serialize_with = "serb64::serialize"
        )]
        pub b01_orig_file_md5: Vec<u8>,
        pub b01_orig_file_size: u64,
        pub b01_width: usize,
        #[serde(
            rename = "b16OrigFileMD5",
            deserialize_with = "serde_bytes::deserialize",
            serialize_with = "serb64::serialize"
        )]
        pub b16_orig_file_md5: Vec<u8>,
        pub b16_orig_file_size: u64,
        pub creation_date: String,
        pub legacy_comment: Option<String>,
        pub legacy_rotation: u16,
        pub mos_num: i8,
        pub scan_num: i8,
        #[serde(
            default,
            deserialize_with = "serde_bytes::deserialize",
            serialize_with = "serb64::serialize"
        )]
        pub result_id: Vec<u8>,
        pub s3_key_template: String,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PlatesPhotometryData {
        pub median_colorterm_apass: Option<f32>,
        pub median_colorterm_atlas: Option<f32>,
        pub n_magdep_apass: Option<u8>,
        pub n_magdep_atlas: Option<u8>,
        pub n_solutions_apass: Option<u8>,
        pub n_solutions_atlas: Option<u8>,

        #[serde(
            default,
            deserialize_with = "serde_bytes::deserialize",
            serialize_with = "serb64::serialize"
        )]
        pub result_id_apass: Vec<u8>,

        #[serde(
            default,
            deserialize_with = "serde_bytes::deserialize",
            serialize_with = "serb64::serialize"
        )]
        pub result_id_atlas: Vec<u8>,
    }
}

/// Types for querying the plates table for the `queryexps` endpoint.
pub mod plates_queryexps {
    use super::*;

    pub const PROJECTION_EXPRESSION: &str = "\
        astrometry.b01HeaderGz,\
        astrometry.exposures,\
        astrometry.nSolutions,\
        astrometry.rotationDelta,\
        mosaic.b01Height,\
        mosaic.b01Width,\
        mosaic.creationDate,\
        mosaic.mosNum,\
        mosaic.scanNum,\
        photometry.medianColortermApass,\
        photometry.medianColortermAtlas,\
        photometry.nMagdepApass,\
        photometry.nMagdepAtlas,\
        photometry.nSolutionsApass,\
        photometry.nSolutionsAtlas,\
        photometry.resultIdApass,\
        photometry.resultIdAtlas,\
        plateId,\
        plateNumber,\
        series";

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PlatesResult {
        pub astrometry: Option<PlatesAstrometryResult>,
        pub mosaic: Option<PlatesMosaicResult>,
        pub photometry: Option<PlatesPhotometryData>,
        pub plate_id: String,
        pub plate_number: usize,
        pub series: String,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PlatesAstrometryResult {
        #[serde(default, with = "serde_bytes")]
        // should be Option<>, but not sure how to nest the custom deserializer
        pub b01_header_gz: Vec<u8>,
        pub n_solutions: Option<usize>,
        pub rotation_delta: Option<isize>,
        pub exposures: Vec<Option<PlatesExposureResult>>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PlatesExposureResult {
        pub center_source: Option<String>,
        //date_acc_days: Option<f64>,
        //date_source: Option<String>,
        pub dec_deg: Option<f64>,
        pub dur_min: Option<f64>,
        pub midpoint_date: Option<String>,
        pub number: i8,
        pub ra_deg: Option<f64>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PlatesMosaicResult {
        pub b01_height: usize,
        pub b01_width: usize,
        pub creation_date: String,
        pub mos_num: i8,
        pub scan_num: i8,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PlatesPhotometryData {
        pub median_colorterm_apass: Option<f32>,
        pub median_colorterm_atlas: Option<f32>,
        pub n_magdep_apass: Option<u8>,
        pub n_magdep_atlas: Option<u8>,
        pub n_solutions_apass: Option<u8>,
        pub n_solutions_atlas: Option<u8>,

        #[serde(default, with = "serde_bytes")]
        // should be Option<>, but not sure how to nest the custom deserializer
        pub result_id_apass: Vec<u8>,

        #[serde(default, with = "serde_bytes")]
        // should be Option<>, but not sure how to nest the custom deserializer
        pub result_id_atlas: Vec<u8>,
    }
}

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
