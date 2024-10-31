//! Dealing with the DASCH photometry data.

use binary_serde::{BinarySerde, Endianness};
use lambda_http::Error;

use crate::{gscbin::GscBinning, mosaics::PLATE_SERIES_BY_ID, BUCKET};

/// A record in the binary "magfiles" that store compiled DASCH photometry data.
/// We cannot rearrange any fields here -- this struct captures the format used
/// by the legacy C code.
#[derive(BinarySerde, Debug, PartialEq)]
pub struct MagRecord {
    marker: u32,
    version: u32,
    pub ref_number: u64,
    x_image: f64,
    y_image: f64,
    mag_iso: f64,
    ra_deg: f64,
    dec_deg: f64,
    date_jd: f64,
    flux_iso: f64,
    mag_aper: f64,
    mag_auto: f64,
    kron_radius: f64,
    background: f64,
    flux_max_adu: f64,
    theta_j2000: f64,
    ellipticity: f64,
    iso_area_sqdeg: f64,
    fwhm_pix: f64,
    fwhm_deg: f64,
    plate_center_dist_deg: f64,
    blended_mag: f64,
    drad_rms2: f64,
    ra_cat_corrected: f64,
    dec_cat_corrected: f64,
    magcal_iso: f32,
    magcal_iso_rms: f32,
    magcal_local: f32,
    magcal_local_rms: f32,
    limiting_mag_local: f32,
    magcal_local_error: f32,
    magcor_local: f32,
    extinction: f32,
    magcal_magdep: f32,
    magcal_magdep_rms: f32,
    pm_ra_masyr: f32,
    pm_dec_masyr: f32,
    time_accuracy_days: f32,
    gsc_bin_index: u32,
    pub plate_number: u32,
    sextractor_number: u32,
    version_id: u32,
    aflags: u32,
    a2flags: u32,
    bflags: u32,
    b2flags: u32,
    iso_areas: [u32; 8],
    npoints_local: u32,
    reject_flag: u32,
    magdep_bin: i32,
    pass_bits: u16,
    local_bin_index: u16,
    mask_index: u16,
    pub series_id: u8,
    exposure_number: i8,
    pub solution_number: u8,
    spatial_bin: u8,
    catalog_number: u8,
    padding: [u8; 5],
}

fn nanflag64(x: f64, flagval: f64) -> f64 {
    if x == flagval {
        f64::NAN
    } else {
        x
    }
}

fn nanflag64_2(x: f64, flagval1: f64, flagval2: f64) -> f64 {
    if x == flagval1 || x == flagval2 {
        f64::NAN
    } else {
        x
    }
}

fn nanflag32(x: f32, flagval: f32) -> f32 {
    if x == flagval {
        f32::NAN
    } else {
        x
    }
}

fn negflag32(x: f32) -> f32 {
    if x <= 0. {
        f32::NAN
    } else {
        x
    }
}

impl MagRecord {
    /// Consume this magfile record and convert it into an output record.
    ///
    /// Here is where we also convert flag float values to NaNs, to the extent
    /// possible.
    pub fn into_output(self, mosaic_number: i8) -> OutputRecord {
        OutputRecord {
            date_jd: self.date_jd,
            series_id: self.series_id,
            plate_number: self.plate_number,
            mosaic_number,
            solution_number: self.solution_number,
            limiting_mag_local: self.limiting_mag_local,
            det: Some(OutputDetection {
                ref_number: self.ref_number,
                x_image: self.x_image,
                y_image: self.y_image,
                mag_iso: self.mag_iso,
                ra_deg: self.ra_deg,
                dec_deg: self.dec_deg,
                flux_iso: self.flux_iso,
                mag_aper: self.mag_aper,
                mag_auto: self.mag_auto,
                kron_radius: self.kron_radius,
                background: self.background,
                flux_max_adu: self.flux_max_adu,
                theta_j2000: self.theta_j2000,
                ellipticity: self.ellipticity,
                iso_area_sqdeg: self.iso_area_sqdeg,
                fwhm_pix: self.fwhm_pix,
                fwhm_deg: self.fwhm_deg,
                plate_center_dist_deg: self.plate_center_dist_deg,
                blended_mag: nanflag64_2(self.blended_mag, 0., 99.),
                drad_rms2: nanflag64(self.drad_rms2, 99.),
                ra_cat_corrected: nanflag64(self.ra_cat_corrected, 999.),
                dec_cat_corrected: nanflag64(self.dec_cat_corrected, 99.),
                magcal_iso: self.magcal_iso,
                magcal_iso_rms: nanflag32(self.magcal_iso_rms, 99.),
                magcal_local: self.magcal_local,
                magcal_local_rms: nanflag32(self.magcal_local_rms, 99.),
                magcal_local_error: self.magcal_local_error,
                magcor_local: nanflag32(self.magcor_local, 0.),
                extinction: self.extinction,
                magcal_magdep: self.magcal_magdep,
                magcal_magdep_rms: nanflag32(self.magcal_magdep_rms, 99.),
                pm_ra_masyr: nanflag32(self.pm_ra_masyr, 999999.),
                pm_dec_masyr: nanflag32(self.pm_dec_masyr, 999999.),
                time_accuracy_days: negflag32(self.time_accuracy_days),
                gsc_bin_index: self.gsc_bin_index,
                sextractor_number: self.sextractor_number,
                version_id: self.version_id,
                aflags: self.aflags,
                a2flags: self.a2flags,
                bflags: self.bflags,
                b2flags: self.b2flags,
                iso_areas: self.iso_areas,
                npoints_local: self.npoints_local,
                reject_flag: self.reject_flag,
                magdep_bin: self.magdep_bin,
                pass_bits: self.pass_bits,
                local_bin_index: self.local_bin_index,
                mask_index: self.mask_index,
                exposure_number: self.exposure_number,
                spatial_bin: self.spatial_bin,
                catalog_number: self.catalog_number,
            }),
        }
    }
}

/// An output record for APIs that provide photometry data (`lightcurve`,
/// `platephot`). This is *almost* identical to `MagRecord`, but not quite. It
/// adds quality-of-life fixes like a mosaic_number field, which the magfile
/// data don't have, and the plate series as a string. It also allows for
/// upper-limit records.
#[derive(Debug, PartialEq)]
pub struct OutputRecord {
    pub date_jd: f64,
    series_id: u8,
    plate_number: u32,
    pub mosaic_number: i8,
    solution_number: u8,
    limiting_mag_local: f32,

    // Could consider boxing this to save memory for non-detections, but, meh.
    det: Option<OutputDetection>,
}

#[derive(Debug, PartialEq)]
pub struct OutputDetection {
    ref_number: u64,
    x_image: f64,
    y_image: f64,
    mag_iso: f64,
    ra_deg: f64,
    dec_deg: f64,
    flux_iso: f64,
    mag_aper: f64,
    mag_auto: f64,
    kron_radius: f64,
    background: f64,
    flux_max_adu: f64,
    theta_j2000: f64,
    ellipticity: f64,
    iso_area_sqdeg: f64,
    fwhm_pix: f64,
    fwhm_deg: f64,
    plate_center_dist_deg: f64,
    blended_mag: f64,
    drad_rms2: f64,
    ra_cat_corrected: f64,
    dec_cat_corrected: f64,
    magcal_iso: f32,
    magcal_iso_rms: f32,
    magcal_local: f32,
    magcal_local_rms: f32,
    magcal_local_error: f32,
    magcor_local: f32,
    extinction: f32,
    magcal_magdep: f32,
    magcal_magdep_rms: f32,
    pm_ra_masyr: f32,
    pm_dec_masyr: f32,
    time_accuracy_days: f32,
    gsc_bin_index: u32,
    sextractor_number: u32,
    version_id: u32,
    aflags: u32,
    a2flags: u32,
    bflags: u32,
    b2flags: u32,
    iso_areas: [u32; 8],
    npoints_local: u32,
    reject_flag: u32,
    magdep_bin: i32,
    pass_bits: u16,
    local_bin_index: u16,
    mask_index: u16,
    exposure_number: i8,
    spatial_bin: u8,
    catalog_number: u8,
}

fn nanf64(x: f64) -> String {
    if x.is_nan() {
        String::new()
    } else {
        x.to_string()
    }
}

fn nanf32(x: f32) -> String {
    if x.is_nan() {
        String::new()
    } else {
        x.to_string()
    }
}

fn flag<T: PartialEq + ToString>(x: T, flagval: T) -> String {
    if x == flagval {
        String::default()
    } else {
        x.to_string()
    }
}

impl OutputRecord {
    /// Get a CSV header row appropriate for CSV-format display of output
    /// records.
    ///
    /// This must be kept in sync with `as_csv_row()`, of course.
    pub fn csv_header() -> String {
        "\
        date_jd,\
        series,\
        plate_number,\
        mosaic_number,\
        solution_number,\
        limiting_mag_local,\
        ref_number,\
        x_image,\
        y_image,\
        mag_iso,\
        ra_deg,\
        dec_deg,\
        flux_iso,\
        mag_aper,\
        mag_auto,\
        kron_radius,\
        background,\
        flux_max_adu,\
        theta_j2000,\
        ellipticity,\
        iso_area_sqdeg,\
        fwhm_pix,\
        fwhm_deg,\
        plate_center_dist_deg,\
        blended_mag,\
        drad_rms2,\
        ra_cat_corrected,\
        dec_cat_corrected,\
        magcal_iso,\
        magcal_iso_rms,\
        magcal_local,\
        magcal_local_rms,\
        magcal_local_error,\
        magcor_local,\
        extinction,\
        magcal_magdep,\
        magcal_magdep_rms,\
        pm_ra_masyr,\
        pm_dec_masyr,\
        time_accuracy_days,\
        gsc_bin_index,\
        sextractor_number,\
        version_id,\
        aflags,\
        a2flags,\
        bflags,\
        b2flags,\
        iso_area_0,\
        iso_area_1,\
        iso_area_2,\
        iso_area_3,\
        iso_area_4,\
        iso_area_5,\
        iso_area_6,\
        iso_area_7,\
        npoints_local,\
        reject_flag,\
        magdep_bin,\
        pass_bits,\
        local_bin_index,\
        mask_index,\
        exposure_number,\
        spatial_bin,\
        catalog_number"
            .to_owned()
    }

    /// Turn this into a CSV row.
    pub fn as_csv_row(&self) -> String {
        let mut cells = Vec::new();

        cells.push(nanf64(self.date_jd));
        cells.push(PLATE_SERIES_BY_ID[self.series_id as usize].to_string());
        cells.push(self.plate_number.to_string());
        cells.push(self.mosaic_number.to_string());
        cells.push(self.solution_number.to_string());
        cells.push(nanf32(self.limiting_mag_local));

        if let Some(d) = self.det.as_ref() {
            cells.push(flag(d.ref_number, 0));
            cells.push(nanf64(d.x_image));
            cells.push(nanf64(d.y_image));
            cells.push(nanf64(d.mag_iso));
            cells.push(nanf64(d.ra_deg));
            cells.push(nanf64(d.dec_deg));
            cells.push(nanf64(d.flux_iso));
            cells.push(nanf64(d.mag_aper));
            cells.push(nanf64(d.mag_auto));
            cells.push(nanf64(d.kron_radius));
            cells.push(nanf64(d.background));
            cells.push(nanf64(d.flux_max_adu));
            cells.push(nanf64(d.theta_j2000));
            cells.push(nanf64(d.ellipticity));
            cells.push(nanf64(d.iso_area_sqdeg));
            cells.push(nanf64(d.fwhm_pix));
            cells.push(nanf64(d.fwhm_deg));
            cells.push(nanf64(d.plate_center_dist_deg));
            cells.push(nanf64(d.blended_mag));
            cells.push(nanf64(d.drad_rms2));
            cells.push(nanf64(d.ra_cat_corrected));
            cells.push(nanf64(d.dec_cat_corrected));
            cells.push(nanf32(d.magcal_iso));
            cells.push(nanf32(d.magcal_iso_rms));
            cells.push(nanf32(d.magcal_local));
            cells.push(nanf32(d.magcal_local_rms));
            cells.push(nanf32(d.magcal_local_error));
            cells.push(nanf32(d.magcor_local));
            cells.push(nanf32(d.extinction));
            cells.push(nanf32(d.magcal_magdep));
            cells.push(nanf32(d.magcal_magdep_rms));
            cells.push(nanf32(d.pm_ra_masyr));
            cells.push(nanf32(d.pm_dec_masyr));
            cells.push(nanf32(d.time_accuracy_days));
            cells.push(d.gsc_bin_index.to_string());
            cells.push(d.sextractor_number.to_string());
            cells.push(d.version_id.to_string());
            cells.push(d.aflags.to_string());
            cells.push(d.a2flags.to_string());
            cells.push(d.bflags.to_string());
            cells.push(d.b2flags.to_string());
            cells.push(d.iso_areas[0].to_string());
            cells.push(d.iso_areas[1].to_string());
            cells.push(d.iso_areas[2].to_string());
            cells.push(d.iso_areas[3].to_string());
            cells.push(d.iso_areas[4].to_string());
            cells.push(d.iso_areas[5].to_string());
            cells.push(d.iso_areas[6].to_string());
            cells.push(d.iso_areas[7].to_string());
            cells.push(d.npoints_local.to_string());
            cells.push(d.reject_flag.to_string());
            cells.push(d.magdep_bin.to_string());
            cells.push(d.pass_bits.to_string());
            cells.push(d.local_bin_index.to_string());
            cells.push(d.mask_index.to_string());
            cells.push(d.exposure_number.to_string());
            cells.push(d.spatial_bin.to_string());
            cells.push(d.catalog_number.to_string());
        } else {
            // This is dumb.
            for _ in 0..58 {
                cells.push(String::new());
            }
        }

        cells.join(",")
    }
}

#[derive(BinarySerde, Debug, PartialEq)]
pub struct LimitsPlateRecord {
    rec_type: u32,
    rec_version: u32,
    pub limiting_mag_local: f64,
    geo_jd: f64,
    pub series_id: u32,
    pub plate_number: u32,
    pub mosaic_number: u32,
    pub solution_number: u32,
    version: u32,
    _unused: u32,
}

impl From<LimitsPlateRecord> for OutputRecord {
    fn from(lim: LimitsPlateRecord) -> Self {
        OutputRecord {
            date_jd: lim.geo_jd,
            series_id: lim.series_id as u8,
            plate_number: lim.plate_number,
            mosaic_number: lim.mosaic_number as i8,
            solution_number: lim.solution_number as u8,
            limiting_mag_local: lim.limiting_mag_local as f32,
            det: None,
        }
    }
}

#[derive(BinarySerde, Debug, PartialEq)]
struct LimitsIndexRecord {
    byte_offset: u64,
    byte_length: u64,
}

/// Given a sky position, retrieve the limiting-magnitude data associated with
/// it.
///
/// The return value will be a buffer of the binary data, which can be unpacked
/// into LimitsPlateRecord values.
pub async fn get_limiting_records(
    refcat: &str,
    ra_deg: f64,
    dec_deg: f64,
    s3: &aws_sdk_s3::Client,
    bin2: &GscBinning,
) -> Result<Vec<u8>, Error> {
    let dec_bin = bin2.get_dec_bin(dec_deg);
    let total_bin = bin2.get_total_bin(dec_bin, ra_deg);

    // Look up this bin in the index

    let start_byte = total_bin * LimitsIndexRecord::SERIALIZED_SIZE;
    let s3_key = format!("dasch-dr7-phot-{}/lim_aws.idx", refcat);
    let data = s3
        .get_object()
        .bucket(BUCKET)
        .key(&s3_key)
        .range(format!(
            "bytes={}-{}",
            start_byte,
            start_byte + LimitsIndexRecord::SERIALIZED_SIZE - 1
        ))
        .send()
        .await?
        .body
        .collect()
        .await?
        .to_vec();

    let rec = LimitsIndexRecord::binary_deserialize(&data, Endianness::Little)
        .map_err(|e| -> Error { format!("deserialize failed: {e}").into() })?;

    // Now we can fetch the actual payload

    let s3_key = format!("dasch-dr7-phot-{}/limiting.dat", refcat);
    Ok(s3
        .get_object()
        .bucket(BUCKET)
        .key(&s3_key)
        .range(format!(
            "bytes={}-{}",
            rec.byte_offset,
            rec.byte_offset + rec.byte_length - 1
        ))
        .send()
        .await?
        .body
        .collect()
        .await?
        .to_vec())
}
