//! The lightcurve retrieval API.

use binary_serde::{BinarySerde, Endianness};
use lambda_http::Error;
use serde::Deserialize;
use serde_json::Value;

use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek},
};

use crate::mosaics::SERIES_BY_ID;

#[derive(BinarySerde, Debug, PartialEq)]
struct MagRecord {
    marker: u32,
    version: u32,
    ref_number: u64,
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
    solution_number: u8,
    spatial_bin: u8,
    catalog_number: u8,
    padding: [u8; 5],
}

#[derive(BinarySerde, Debug, PartialEq)]
struct LimitsPlateRecord {
    rec_type: u32,
    rec_version: u32,
    limiting_mag_local: f64,
    geo_jd: f64,
    series_id: u32,
    plate_number: u32,
    mosaic_number: u32,
    solution_number: u32,
    version: u32,
    _unused: u32,
}

#[derive(Debug, PartialEq)]
struct OutputRecord {
    ref_number: u64,
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
    mosaic_number: i8,
    solution_number: u8,
    spatial_bin: u8,
    catalog_number: u8,
}

impl ToString for OutputRecord {
    /// Custom implementation for our output CSV format
    fn to_string(&self) -> String {
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
        cells.push(SERIES_BY_ID[self.series_id as usize].to_string());

        cells.join(",")
    }
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

impl From<MagRecord> for OutputRecord {
    fn from(mag: MagRecord) -> Self {
        OutputRecord {
            ref_number: mag.ref_number,
            x_image: mag.x_image,
            y_image: mag.y_image,
            mag_iso: mag.mag_iso,
            ra_deg: mag.ra_deg,
            dec_deg: mag.dec_deg,
            date_jd: mag.date_jd,
            flux_iso: mag.flux_iso,
            mag_aper: mag.mag_aper,
            mag_auto: mag.mag_auto,
            kron_radius: mag.kron_radius,
            background: mag.background,
            flux_max_adu: mag.flux_max_adu,
            theta_j2000: mag.theta_j2000,
            ellipticity: mag.ellipticity,
            iso_area_sqdeg: mag.iso_area_sqdeg,
            fwhm_pix: mag.fwhm_pix,
            fwhm_deg: mag.fwhm_deg,
            plate_center_dist_deg: mag.plate_center_dist_deg,
            blended_mag: mag.blended_mag,
            drad_rms2: mag.drad_rms2,
            ra_cat_corrected: mag.ra_cat_corrected,
            dec_cat_corrected: mag.dec_cat_corrected,
            magcal_iso: mag.magcal_iso,
            magcal_iso_rms: mag.magcal_iso_rms,
            magcal_local: mag.magcal_local,
            magcal_local_rms: mag.magcal_local_rms,
            limiting_mag_local: mag.limiting_mag_local,
            magcal_local_error: mag.magcal_local_error,
            magcor_local: mag.magcor_local,
            extinction: mag.extinction,
            magcal_magdep: mag.magcal_magdep,
            magcal_magdep_rms: mag.magcal_magdep_rms,
            pm_ra_masyr: mag.pm_ra_masyr,
            pm_dec_masyr: mag.pm_dec_masyr,
            time_accuracy_days: mag.time_accuracy_days,
            gsc_bin_index: mag.gsc_bin_index,
            plate_number: mag.plate_number,
            sextractor_number: mag.sextractor_number,
            version_id: mag.version_id,
            aflags: mag.aflags,
            a2flags: mag.a2flags,
            bflags: mag.bflags,
            b2flags: mag.b2flags,
            iso_areas: mag.iso_areas,
            npoints_local: mag.npoints_local,
            reject_flag: mag.reject_flag,
            magdep_bin: mag.magdep_bin,
            pass_bits: mag.pass_bits,
            local_bin_index: mag.local_bin_index,
            mask_index: mag.mask_index,
            series_id: mag.series_id,
            exposure_number: mag.exposure_number,
            solution_number: mag.solution_number,
            mosaic_number: 0,
            spatial_bin: mag.spatial_bin,
            catalog_number: mag.catalog_number,
        }
    }
}

/// Sync with `json-schemas/lightcurve_request.json`, which then needs to be
/// synced into S3.
#[derive(Deserialize)]
pub struct Request {
    refcat: String,
    _ref_number: u64,
}

pub async fn handler(
    req: Option<Value>,
    binning: &crate::gscbin::GscBinning,
) -> Result<Value, Error> {
    Ok(serde_json::to_value(
        implementation(
            serde_json::from_value(req.ok_or_else(|| -> Error { "no request payload".into() })?)?,
            binning,
        )
        .await?,
    )?)
}

pub async fn implementation(
    request: Request,
    _binning: &crate::gscbin::GscBinning,
) -> Result<Vec<String>, Error> {
    let mut lines = Vec::new();

    // Validation

    match request.refcat.as_ref() {
        "apass" | "atlas" => {}
        _ => {
            return Err("illegal refcat parameter".into());
        }
    }

    // TEMPORARY SCAFFOLDING WHILE I WORK OUT THE ALGORITHMS

    let ref_number = 9926923921u64;
    let gsc_bin_index = 149103038;
    let ra_deg = 125.047343;
    let dec_deg = 49.899661;
    const MAGFILE_HEADER_SIZE: u64 = 12320;
    const LIMITSFILE_OFFSET: u64 = 23799402232;
    const LIMITSFILE_NBYTES: usize = 196512;
    const LIMITSFILE_NRECS: usize = 4094;

    let mut f = File::open("0149102592.dat")?;
    f.seek(std::io::SeekFrom::Start(MAGFILE_HEADER_SIZE))?;
    let mut buf = vec![0u8; 336];

    let mut rows = HashMap::new();

    loop {
        if let Err(e) = f.read_exact(&mut buf[..]) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            }

            panic!("I/O error: {e}");
        }

        let rec = MagRecord::binary_deserialize(&buf[..], Endianness::Little).unwrap();

        if rec.ref_number == ref_number {
            rows.insert((rec.series_id, rec.plate_number), OutputRecord::from(rec));
        }
    }

    let mut f = File::open("limiting.dat")?;
    f.seek(std::io::SeekFrom::Start(LIMITSFILE_OFFSET))?;
    let mut buf = vec![0u8; 48];

    for _ in 0..LIMITSFILE_NRECS {
        f.read_exact(&mut buf[..]).unwrap();
        let rec = LimitsPlateRecord::binary_deserialize(&buf[..], Endianness::Little).unwrap();
        let key = (rec.series_id as u8, rec.plate_number);
        rows.entry(key).or_insert_with(|| OutputRecord::from(rec));
    }

    let mut rows: Vec<_> = rows.drain().map(|(_k, v)| v).collect();
    rows.sort_by(|a, b| a.date_jd.partial_cmp(&b.date_jd).unwrap());

    lines.push(
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
            .to_owned(),
    );

    for row in rows.drain(..) {
        lines.push(row.to_string());
    }

    Ok(lines)
}
