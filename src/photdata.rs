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

impl MagRecord {
    /// Consume this magfile record and convert it into an output record.
    pub fn into_output(self, mosaic_number: i8) -> OutputRecord {
        OutputRecord {
            ref_number: self.ref_number,
            x_image: self.x_image,
            y_image: self.y_image,
            mag_iso: self.mag_iso,
            ra_deg: self.ra_deg,
            dec_deg: self.dec_deg,
            date_jd: self.date_jd,
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
            blended_mag: self.blended_mag,
            drad_rms2: self.drad_rms2,
            ra_cat_corrected: self.ra_cat_corrected,
            dec_cat_corrected: self.dec_cat_corrected,
            magcal_iso: self.magcal_iso,
            magcal_iso_rms: self.magcal_iso_rms,
            magcal_local: self.magcal_local,
            magcal_local_rms: self.magcal_local_rms,
            limiting_mag_local: self.limiting_mag_local,
            magcal_local_error: self.magcal_local_error,
            magcor_local: self.magcor_local,
            extinction: self.extinction,
            magcal_magdep: self.magcal_magdep,
            magcal_magdep_rms: self.magcal_magdep_rms,
            pm_ra_masyr: self.pm_ra_masyr,
            pm_dec_masyr: self.pm_dec_masyr,
            time_accuracy_days: self.time_accuracy_days,
            gsc_bin_index: self.gsc_bin_index,
            plate_number: self.plate_number,
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
            series_id: self.series_id,
            exposure_number: self.exposure_number,
            solution_number: self.solution_number,
            mosaic_number: mosaic_number,
            spatial_bin: self.spatial_bin,
            catalog_number: self.catalog_number,
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
    ref_number: u64,
    x_image: f64,
    y_image: f64,
    mag_iso: f64,
    ra_deg: f64,
    dec_deg: f64,
    pub date_jd: f64,
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
    plate_number: u32,
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
    series_id: u8,
    exposure_number: i8,
    pub mosaic_number: i8,
    solution_number: u8,
    spatial_bin: u8,
    catalog_number: u8,
}

impl OutputRecord {
    /// Get a CSV header row appropriate for CSV-format display of output
    /// records.
    ///
    /// This must be kept in sync with `as_csv_row()`, of course.
    pub fn csv_header() -> String {
        "\
        ref_number,\
        x_image,\
        y_image,\
        mag_iso,\
        ra_deg,\
        dec_deg,\
        date_jd,\
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
        limiting_mag_local,\
        magcal_local_error,\
        magcor_local,\
        extinction,\
        magcal_magdep,\
        magcal_magdep_rms,\
        pm_ra_masyr,\
        pm_dec_masyr,\
        time_accuracy_days,\
        gsc_bin_index,\
        plate_number,\
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
        series_id,\
        exposure_number,\
        mosaic_number,\
        solution_number,\
        spatial_bin,\
        catalog_number,\
        series"
            .to_owned()
    }

    /// Turn this into a CSV row.
    pub fn as_csv_row(&self) -> String {
        let mut cells = Vec::new();

        cells.push(self.ref_number.to_string());
        cells.push(self.x_image.to_string());
        cells.push(self.y_image.to_string());
        cells.push(self.mag_iso.to_string());
        cells.push(self.ra_deg.to_string());
        cells.push(self.dec_deg.to_string());
        cells.push(self.date_jd.to_string());
        cells.push(self.flux_iso.to_string());
        cells.push(self.mag_aper.to_string());
        cells.push(self.mag_auto.to_string());
        cells.push(self.kron_radius.to_string());
        cells.push(self.background.to_string());
        cells.push(self.flux_max_adu.to_string());
        cells.push(self.theta_j2000.to_string());
        cells.push(self.ellipticity.to_string());
        cells.push(self.iso_area_sqdeg.to_string());
        cells.push(self.fwhm_pix.to_string());
        cells.push(self.fwhm_deg.to_string());
        cells.push(self.plate_center_dist_deg.to_string());
        cells.push(self.blended_mag.to_string());
        cells.push(self.drad_rms2.to_string());
        cells.push(self.ra_cat_corrected.to_string());
        cells.push(self.dec_cat_corrected.to_string());
        cells.push(self.magcal_iso.to_string());
        cells.push(self.magcal_iso_rms.to_string());
        cells.push(self.magcal_local.to_string());
        cells.push(self.magcal_local_rms.to_string());
        cells.push(self.limiting_mag_local.to_string());
        cells.push(self.magcal_local_error.to_string());
        cells.push(self.magcor_local.to_string());
        cells.push(self.extinction.to_string());
        cells.push(self.magcal_magdep.to_string());
        cells.push(self.magcal_magdep_rms.to_string());
        cells.push(self.pm_ra_masyr.to_string());
        cells.push(self.pm_dec_masyr.to_string());
        cells.push(self.time_accuracy_days.to_string());
        cells.push(self.gsc_bin_index.to_string());
        cells.push(self.plate_number.to_string());
        cells.push(self.sextractor_number.to_string());
        cells.push(self.version_id.to_string());
        cells.push(self.aflags.to_string());
        cells.push(self.a2flags.to_string());
        cells.push(self.bflags.to_string());
        cells.push(self.b2flags.to_string());
        cells.push(self.iso_areas[0].to_string());
        cells.push(self.iso_areas[1].to_string());
        cells.push(self.iso_areas[2].to_string());
        cells.push(self.iso_areas[3].to_string());
        cells.push(self.iso_areas[4].to_string());
        cells.push(self.iso_areas[5].to_string());
        cells.push(self.iso_areas[6].to_string());
        cells.push(self.iso_areas[7].to_string());
        cells.push(self.npoints_local.to_string());
        cells.push(self.reject_flag.to_string());
        cells.push(self.magdep_bin.to_string());
        cells.push(self.pass_bits.to_string());
        cells.push(self.local_bin_index.to_string());
        cells.push(self.mask_index.to_string());
        cells.push(self.series_id.to_string());
        cells.push(self.exposure_number.to_string());
        cells.push(self.mosaic_number.to_string());
        cells.push(self.solution_number.to_string());
        cells.push(self.spatial_bin.to_string());
        cells.push(self.catalog_number.to_string());
        cells.push(PLATE_SERIES_BY_ID[self.series_id as usize].to_string());

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
            ref_number: 0,
            x_image: f64::NAN,
            y_image: f64::NAN,
            mag_iso: f64::NAN,
            ra_deg: f64::NAN,
            dec_deg: f64::NAN,
            date_jd: lim.geo_jd,
            flux_iso: f64::NAN,
            mag_aper: f64::NAN,
            mag_auto: f64::NAN,
            kron_radius: f64::NAN,
            background: f64::NAN,
            flux_max_adu: f64::NAN,
            theta_j2000: f64::NAN,
            ellipticity: f64::NAN,
            iso_area_sqdeg: f64::NAN,
            fwhm_pix: f64::NAN,
            fwhm_deg: f64::NAN,
            plate_center_dist_deg: f64::NAN,
            blended_mag: f64::NAN,
            drad_rms2: f64::NAN,
            ra_cat_corrected: f64::NAN,
            dec_cat_corrected: f64::NAN,
            magcal_iso: f32::NAN,
            magcal_iso_rms: f32::NAN,
            magcal_local: f32::NAN,
            magcal_local_rms: f32::NAN,
            limiting_mag_local: lim.limiting_mag_local as f32,
            magcal_local_error: f32::NAN,
            magcor_local: f32::NAN,
            extinction: f32::NAN,
            magcal_magdep: f32::NAN,
            magcal_magdep_rms: f32::NAN,
            pm_ra_masyr: f32::NAN,
            pm_dec_masyr: f32::NAN,
            time_accuracy_days: f32::NAN,
            gsc_bin_index: 0,
            plate_number: lim.plate_number,
            sextractor_number: 0,
            version_id: 0,
            aflags: 0,
            a2flags: 0,
            bflags: 0,
            b2flags: 0,
            iso_areas: [0; 8],
            npoints_local: 0,
            reject_flag: 0,
            magdep_bin: 0,
            pass_bits: 0,
            local_bin_index: 0,
            mask_index: 0,
            series_id: lim.series_id as u8,
            exposure_number: -1,
            solution_number: lim.solution_number as u8,
            mosaic_number: lim.mosaic_number as i8,
            spatial_bin: 0,
            catalog_number: 0,
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
