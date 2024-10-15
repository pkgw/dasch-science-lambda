//! The cutout API service
//!
//! This is probably the most sophisticated service that we have. We query
//! DynamoDB to get information about a plate and its astrometry; then use
//! wcslib to figure out what part of the full-size plate we need to sample;
//! then stream data from the compressed FITS file on S3; then resample onto the
//! target coordinate system.
//!
//! Fortunately, our resulting cutout size stays within the 6 MB limit given to
//! buffered Lambdas, which means we can operate in the cheaper buffered mode.
//! The result of a buffered Lambda can only be JSON, so we return a complete
//! gzipped FITS file as a Base64-encoded string.

use aws_sdk_dynamodb::types::AttributeValue;
use base64::{engine::general_purpose::STANDARD, write::EncoderWriter};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use lambda_http::Error;
use ndarray::{s, Array, Axis, Ix2};
use ndarray_interp::interp2d;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    fitsfile::FitsFile,
    mosaics::{load_b01_header, wcslib_solnum},
    BUCKET,
};

/// Sync with `json-schemas/cutout_request.json`, which then needs to be
/// synced into S3.
#[derive(Deserialize)]
pub struct Request {
    plate_id: String,
    solution_number: usize,
    center_ra_deg: f64,
    center_dec_deg: f64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesResult {
    astrometry: Option<PlatesAstrometryResult>,
    mosaic: Option<PlatesMosaicResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesAstrometryResult {
    #[serde(with = "serde_bytes")]
    b01_header_gz: Vec<u8>,
    n_solutions: usize,
    rotation_delta: isize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesMosaicResult {
    b01_height: usize,
    b01_width: usize,
    s3_key_template: String,
}

const OUTPUT_IMAGE_HALFSIZE: usize = 417;
const OUTPUT_IMAGE_FULLSIZE: usize = 2 * OUTPUT_IMAGE_HALFSIZE + 1;
const OUTPUT_IMAGE_NPIX: usize = OUTPUT_IMAGE_FULLSIZE * OUTPUT_IMAGE_FULLSIZE;
const OUTPUT_IMAGE_PIXSCALE: f64 = 0.0004; // deg/pix

pub async fn handler(req: Option<Value>, dc: &aws_sdk_dynamodb::Client) -> Result<Value, Error> {
    Ok(serde_json::to_value(
        implementation(
            serde_json::from_value(req.ok_or_else(|| -> Error { "no request payload".into() })?)?,
            dc,
        )
        .await?,
    )?)
}

pub async fn implementation(
    request: Request,
    dc: &aws_sdk_dynamodb::Client,
) -> Result<String, Error> {
    // Early validation, with NaN-sensitive logic

    if !(request.center_ra_deg >= 0. && request.center_ra_deg <= 360.) {
        return Err("illegal center_ra_deg parameter".into());
    }

    if !(request.center_dec_deg >= -90. && request.center_dec_deg <= 90.) {
        return Err("illegal center_dec_deg parameter".into());
    }

    // Get the information we need about this plate and validate the basic request.

    let plates_table = format!("dasch-{}-dr7-plates", super::ENVIRONMENT);

    let result = dc
        .get_item()
        .table_name(plates_table)
        .key("plateId", AttributeValue::S(request.plate_id.clone()))
        .projection_expression(
            "astrometry.b01HeaderGz,\
            astrometry.nSolutions,\
            astrometry.rotationDelta,\
            mosaic.b01Height,\
            mosaic.b01Width,\
            mosaic.s3KeyTemplate",
        )
        .send()
        .await?;

    let item = result
        .item
        .ok_or_else(|| -> Error { format!("no such plate_id `{}`", request.plate_id).into() })?;

    let item: PlatesResult = serde_dynamo::from_item(item)?;
    let mos_data = item.mosaic.ok_or_else(|| -> Error {
        format!(
            "plate `{}` has no registered FITS mosaic information (never scanned?)",
            request.plate_id
        )
        .into()
    })?;
    let astrom_data = item.astrometry.ok_or_else(|| -> Error {
        format!(
            "plate `{}` has no registered astrometric solutions",
            request.plate_id
        )
        .into()
    })?;

    if request.solution_number >= astrom_data.n_solutions {
        return Err(format!(
            "requested astrometric solution #{} (0-based) for plate `{}` but it only has {} solutions",
            request.solution_number,
            request.plate_id,
            astrom_data.n_solutions
        )
        .into());
    }

    // IMPLEMENT THESE:

    if astrom_data.rotation_delta != 0 {
        return Err(format!(
            "XXX rotation_delta {} for plate `{}`",
            astrom_data.rotation_delta, request.plate_id,
        )
        .into());
    }

    // We can compute the target WCS and start building the output FITS.
    //
    // TODO: add lots more headers, including approximate WCS for the other
    // exposures on this plate.

    let mut dest_fits = FitsFile::create_mem()?;
    dest_fits.write_square_image_header(OUTPUT_IMAGE_FULLSIZE as u64)?;
    dest_fits.set_string_header("CTYPE1", "RA---TAN")?;
    dest_fits.set_string_header("CTYPE2", "DEC--TAN")?;
    dest_fits.set_string_header("CUNIT1", "deg")?;
    dest_fits.set_string_header("CUNIT2", "deg")?;
    dest_fits.set_f64_header("CRVAL1", request.center_ra_deg)?;
    dest_fits.set_f64_header("CRVAL2", request.center_dec_deg)?;
    dest_fits.set_f64_header("CD1_1", -OUTPUT_IMAGE_PIXSCALE)?;
    dest_fits.set_f64_header("CD2_2", OUTPUT_IMAGE_PIXSCALE)?;
    dest_fits.set_f64_header("CRPIX1", OUTPUT_IMAGE_HALFSIZE as f64 + 1.)?; // 1-based pixel coords
    dest_fits.set_f64_header("CRPIX2", OUTPUT_IMAGE_HALFSIZE as f64 + 1.)?;

    let dest_world = {
        let mut dest_wcs = dest_fits.get_wcs()?;
        dest_wcs
            .get(0)
            .unwrap()
            .sample_world_square(OUTPUT_IMAGE_FULLSIZE)?
    };

    // Figure out where we land on the source image.

    let destpix = {
        let mut src_wcs = load_b01_header(GzDecoder::new(&astrom_data.b01_header_gz[..]))?;
        let wsn = wcslib_solnum(request.solution_number, astrom_data.n_solutions)?;
        src_wcs.get(wsn)?.world_to_pixel(dest_world)?
    };

    let dp_flat = destpix.view().into_shape((OUTPUT_IMAGE_NPIX, 2)).unwrap();
    let mins = dp_flat.map_axis(Axis(0), |view| {
        view.into_iter().copied().reduce(f64::min).unwrap()
    });
    let maxs = dp_flat.map_axis(Axis(0), |view| {
        view.into_iter().copied().reduce(f64::max).unwrap()
    });

    let xmin = isize::max(mins[0].floor() as isize, 0) as usize;
    let xmax = isize::min(maxs[0].ceil() as isize, mos_data.b01_width as isize - 1) as usize;
    let ymin = isize::max(mins[1].floor() as isize, 0) as usize;
    let ymax = isize::min(maxs[1].ceil() as isize, mos_data.b01_height as isize - 1) as usize;

    let src_nx = xmax + 1 - xmin;
    let src_ny = ymax + 1 - ymin;

    if src_nx < 1 || src_ny < 1 {
        return Err(format!(
            "plate `{}` solnum {} does not overlap the target region",
            request.plate_id, request.solution_number,
        )
        .into());
    }

    // Actually get the source pixels.
    //
    // Gross: as far as I can see, since we're bridging across C code, the
    // CFITSIO S3 I/O callbacks can't leverage the main async runtime even
    // though they in turn call async code. I believe that we need to create
    // this "blocking" wrapper thread, which in turn creates its own runtime and
    // does the S3 work.

    eprintln!(
        "to fetch: {} rows, {} cols, {} total pixels",
        src_ny,
        src_nx,
        src_nx * src_ny
    );

    let s3path = mos_data
        .s3_key_template
        .replace("{bin}", "01")
        .replace("{tnx}", "_tnx");
    let s3url = format!("s3://{BUCKET}/{s3path}");

    let src_data = tokio::task::spawn_blocking(move || -> Result<Array<i16, Ix2>, Error> {
        let mut fits = FitsFile::open(s3url)?;
        fits.move_to_hdu(1)?;
        Ok(fits.read_rectangle(xmin, ymin, src_nx, src_ny)?)
    })
    .await??;

    // Perform the interpolation
    //
    // ndarray_interp requires that the x, y, and data types must all be the
    // same. So we have to translate our image data to f64.
    //
    // Also note that its "x" and "y" terminology is such that 2D arrays are
    // indexed `arr[x,y]`, which is the opposite of our convention.

    let xs = destpix
        .slice(s![.., .., 0])
        .to_owned()
        .into_shape(OUTPUT_IMAGE_NPIX)
        .unwrap()
        - xmin as f64;
    let ys = destpix
        .slice(s![.., .., 1])
        .to_owned()
        .into_shape(OUTPUT_IMAGE_NPIX)
        .unwrap()
        - ymin as f64;

    let src_data = src_data.mapv(|e| e as f64);
    let interp = interp2d::Interp2DBuilder::new(src_data).build()?;
    let mut dest_data: Array<f64, _> = Array::zeros(xs.len());
    interp.interp_array_into(&ys, &xs, dest_data.view_mut())?;
    let dest_data = dest_data
        .into_shape((OUTPUT_IMAGE_FULLSIZE, OUTPUT_IMAGE_FULLSIZE))
        .unwrap();
    let dest_data = dest_data.mapv(|e| e as i16);

    // Write out the pixels, and we're done.
    //
    // Buffered lambdas can only emit JSON values. We emit the result as a
    // single string, which is a base64-encoded form of the output file. That
    // file is itself gzipped. So to get uncompressed FITS from the output of
    // this API, you have to decode JSON -> un-base64 -> un-gzip.

    dest_fits.write_pixels(&dest_data)?;

    let mut dest_gz_b64 = Vec::new();

    {
        let dest_gz = EncoderWriter::new(&mut dest_gz_b64, &STANDARD);
        let mut dest = GzEncoder::new(dest_gz, Compression::default());
        dest_fits.into_stream(&mut dest)?;
    }

    let dest_gz_b64 = String::from_utf8(dest_gz_b64)?;
    Ok(dest_gz_b64)
}
