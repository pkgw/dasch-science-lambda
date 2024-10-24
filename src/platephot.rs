//! The per-plate photometry retrieval API.

use aws_sdk_dynamodb::types::AttributeValue;
use binary_serde::{BinarySerde, Endianness};
use lambda_http::Error;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    gscbin::{GscBinning, D2R},
    photdata::{MagRecord, OutputRecord},
    BUCKET,
};

const HALFSIZE_DEG: f64 = 10. / 60.; // 10 arcmin

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesResult {
    astrometry: Option<PlatesAstrometryResult>,
    mosaic: Option<PlatesMosaicResult>,
    plate_number: usize,
    series: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesAstrometryResult {
    n_solutions: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlatesMosaicResult {
    mos_num: i8,
}

/// Sync with `json-schemas/platephot_request.json`, which then needs to be
/// synced into S3.
#[derive(Deserialize)]
pub struct Request {
    refcat: String,
    plate_id: String,
    solution_number: usize,
    center_ra_deg: f64,
    center_dec_deg: f64,
}

pub async fn handler(
    req: Option<Value>,
    dc: &aws_sdk_dynamodb::Client,
    s3: &aws_sdk_s3::Client,
    bin64: &GscBinning,
) -> Result<Value, Error> {
    Ok(serde_json::to_value(
        implementation(
            serde_json::from_value(req.ok_or_else(|| -> Error { "no request payload".into() })?)?,
            dc,
            s3,
            bin64,
        )
        .await?,
    )?)
}

pub async fn implementation(
    request: Request,
    dc: &aws_sdk_dynamodb::Client,
    s3: &aws_sdk_s3::Client,
    bin64: &GscBinning,
) -> Result<Vec<String>, Error> {
    // Early validation, with NaN-sensitive logic

    match request.refcat.as_ref() {
        "apass" | "atlas" => {}
        _ => {
            return Err("illegal refcat parameter".into());
        }
    }

    if !(request.center_ra_deg >= 0. && request.center_ra_deg <= 360.) {
        return Err("illegal center_ra_deg parameter".into());
    }

    if !(request.center_dec_deg >= -90. && request.center_dec_deg <= 90.) {
        return Err("illegal center_dec_deg parameter".into());
    }

    // Get some information about the target plate. This is barely necessary,
    // but should be cheap to do. It helps us split the ID into series and
    // platenum in a principled way, and gives us the mosaic number, which is
    // not stored in the magfiles. This would be a good place to attach the
    // plate series ID number as well, rather than having our hardcoded lookup
    // table.

    let plates_table = format!("dasch-{}-dr7-plates", super::ENVIRONMENT);

    let result = dc
        .get_item()
        .table_name(plates_table)
        .key("plateId", AttributeValue::S(request.plate_id.clone()))
        .projection_expression(
            "astrometry.nSolutions,\
            mosaic.mosNum,\
            plateNumber,\
            series",
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

    let series_id = *crate::mosaics::PLATE_ID_BY_SERIES
        .get(&item.series)
        .ok_or_else(|| -> Error {
            format!("no series ID table entry for `{}`", item.series).into()
        })?;

    // Figure out which bins we're going to need to examine. We're looking at a
    // box in RA and dec. In the GSC binning scheme, this translates easily to a
    // set of "tranches" of sequential total bin numbers, each within one of a
    // set of consecutive declination bins.

    let dec_min = f64::max(request.center_dec_deg - HALFSIZE_DEG, -90.0);
    let dec_max = f64::min(request.center_dec_deg + HALFSIZE_DEG, 90.0);
    let dbin0 = bin64.get_dec_bin(dec_min);
    let dbin1 = bin64.get_dec_bin(dec_max);
    let mut tranches = Vec::new();

    for dec_bin in dbin0..=dbin1 {
        let dec = bin64.get_dec_bin_center(dec_bin);
        let factor = 1. / f64::cos(dec * D2R);
        let ra_min = request.center_ra_deg - factor * HALFSIZE_DEG;
        let ra_max = request.center_ra_deg + factor * HALFSIZE_DEG;

        let ranges: &[(f64, f64)] = if ra_min <= 0. && ra_max >= 360. {
            &[(0., 360.)]
        } else if ra_min < 0. {
            &[(0., ra_max), (ra_min + 360., 360.)]
        } else if ra_max > 360. {
            &[(0., ra_max - 360.), (ra_min, 360.)]
        } else {
            &[(ra_min, ra_max)]
        };

        for (tranche_ra_min, tranche_ra_max) in ranges {
            let total_bin_0 = bin64.get_total_bin(dec_bin, *tranche_ra_min);
            let total_bin_1 = bin64.get_total_bin(dec_bin, *tranche_ra_max);
            tranches.push((total_bin_0, total_bin_1));
            eprintln!("tranche: {total_bin_0}-{total_bin_1}");
        }
    }

    let total_bin_min = tranches[0].0;
    let total_bin_max = tranches[tranches.len() - 1].1;
    eprintln!("total bin range: {total_bin_min}-{total_bin_max}");

    // Use the mega-index of all photdb files to figure out what we're going to
    // have to retrieve.

    let index_data = read_mega_index(&request.refcat, total_bin_min, total_bin_max, s3).await?;

    // Use the indexing information to convert tranches of total bin numbers to
    // tranches of reads in our photometry database files. These files are
    // grouped by 1024 bins and those boundaries can land anywhere within our
    // tranches. But in other cases, sequential bin numbers *will* live in files
    // sequentially, and we can process them as one chunk without caring about
    // the increasing GSC bin number.

    let chunks = FileRangeBuilder::process(&tranches[..], total_bin_min, &index_data[..]);

    // Now we can actually fetch and process the photometry data.

    let mut lines = Vec::new();
    lines.push(OutputRecord::csv_header());

    let mut buf = index_data; // Might as well reuse this buffer

    for (file_number, start_offset, end_offset) in chunks {
        eprintln!("req: {file_number} {start_offset} {end_offset}");

        buf.clear();
        let n_bytes = (end_offset - start_offset) as usize;

        let s3_key = format!(
            "dasch-dr7-phot-{}/mags/{}.dat",
            &request.refcat, file_number
        );

        let mut result = s3
            .get_object()
            .bucket(BUCKET)
            .key(&s3_key)
            .range(format!("bytes={}-{}", start_offset, end_offset - 1))
            .send()
            .await?;

        // TODO: do this record-at-a-time, with minimal buffering, instead of
        // accumulating these big dumb chunks.

        while let Some(bytes) = result.body.try_next().await? {
            buf.extend_from_slice(&bytes);
        }

        if buf.len() < n_bytes {
            return Err(format!(
                "short photodata S3 read: {} {file_number} {start_offset} {end_offset}",
                &request.refcat
            )
            .into());
        }

        for c in buf.chunks_exact(MagRecord::SERIALIZED_SIZE) {
            let rec = MagRecord::binary_deserialize(c, Endianness::Little).unwrap();

            if rec.series_id != series_id
                || rec.plate_number as usize != item.plate_number
                || rec.solution_number as usize != request.solution_number
            {
                eprintln!("match: {:?}", rec);
                lines.push(rec.into_output(mos_data.mos_num).as_csv_row());
            }
        }
    }

    Ok(lines)
}

/// Retrieve file offsets from the "mega-index" of where data are sharded among
/// the photdb files.
///
/// total_bin_max is the highest total bin we care about, inclusively. To know
/// the size of its records, we need to fetch the start offset of the next bin.
/// So to get all necessary data for just bin 0, we need to fetch bytes 0-7
/// (inclusive).
async fn read_mega_index(
    refcat: &str,
    total_bin_min: usize,
    total_bin_max: usize,
    s3: &aws_sdk_s3::Client,
) -> Result<Vec<u8>, Error> {
    let start_byte = 4 * total_bin_min;
    let end_byte = 4 * (total_bin_max + 1) + 3;
    let n_bytes = end_byte + 1 - start_byte;
    let mut buf = Vec::with_capacity(n_bytes);

    let s3_key = format!("dasch-dr7-phot-{}/megaindex.dat", refcat);

    let mut result = s3
        .get_object()
        .bucket(BUCKET)
        .key(&s3_key)
        .range(format!("bytes={}-{}", start_byte, end_byte))
        .send()
        .await?;

    while let Some(bytes) = result.body.try_next().await? {
        buf.extend_from_slice(&bytes);
    }

    if buf.len() < n_bytes {
        return Err("couldn't get enough S3 data to service mega-index request".into());
    }

    Ok(buf)
}

/// This is pretty dumb, but lets use reuse the binary-serde machinery.
#[derive(BinarySerde)]
struct Offset(pub u32);

impl Offset {
    /// This might also be dumb, but yes, we treat the stored data as u32s, but
    /// then we convert to i32. That way we get a panic if a size is too big.
    pub fn decode(buf: &[u8]) -> i32 {
        Self::binary_deserialize(&buf[..4], Endianness::Little)
            .unwrap()
            .0 as i32
    }
}

struct FileRangeBuilder {
    chunks: Vec<(isize, i32, i32)>,

    /// File numbers go up to 169 million, so they fit in an isize.
    cur_file_number: isize,

    /// Individual file sizes stay below ~1 GB, so they fit in i32s.
    cur_start_offset: i32,

    /// This is a Python-style (exclusive) range; we read *up to but not
    /// including* the end offset. Other ranges in this module are different
    /// (inclusive, rather than exclusive).
    cur_end_offset: i32,
}

impl FileRangeBuilder {
    /// Whatever chunk we're looking at is *not* contiguous with the chunk that
    /// we've been building up. So, finish up processing of what we've been
    /// working on.
    fn finish_chunk(&mut self) {
        if self.cur_start_offset >= 0 {
            eprintln!(
                "file chunk: {} {} {}",
                self.cur_file_number, self.cur_start_offset, self.cur_end_offset,
            );

            self.chunks.push((
                self.cur_file_number,
                self.cur_start_offset,
                self.cur_end_offset,
            ));
            self.cur_start_offset = -1;
            self.cur_end_offset = -1;
        }
    }

    fn process_one_bin(&mut self, bin_num: usize, total_bin_min: usize, index_data: &[u8]) {
        let this_file_number = (1024 * (bin_num >> 10)) as isize;

        // On to a new file number? Then close out any current chunk.
        if this_file_number != self.cur_file_number {
            self.finish_chunk();
            self.cur_file_number = this_file_number;
        }

        // What are the bounds of the current bin-chunk within the file? We are
        // guaranteed to always be able to peek one past the current bin.
        let ofs = 4 * (bin_num - total_bin_min);
        let bin_start = Offset::decode(&index_data[ofs..]);
        let bin_end = Offset::decode(&index_data[ofs + 4..]);

        if bin_start == self.cur_end_offset {
            // We can coalesce these chunks!
            self.cur_end_offset = bin_end;
        } else {
            // Same file, but different chunk.
            self.finish_chunk();
            self.cur_start_offset = bin_start;
            self.cur_end_offset = bin_end;
        }
    }

    fn process(
        tranches: &[(usize, usize)],
        total_bin_min: usize,
        index_data: &[u8],
    ) -> Vec<(isize, i32, i32)> {
        let mut builder = FileRangeBuilder {
            chunks: Vec::new(),
            cur_file_number: -1,
            cur_start_offset: -1,
            cur_end_offset: -1,
        };

        for tranche in tranches {
            for bin_num in tranche.0..=tranche.1 {
                builder.process_one_bin(bin_num, total_bin_min, index_data);
            }
        }

        builder.finish_chunk();
        builder.chunks
    }
}
