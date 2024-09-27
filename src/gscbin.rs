/// Degree-to-radian conversion factor
pub const D2R: f64 = 0.017453292519943295;

#[derive(Debug)]
pub struct GscBinning {
    bin_size: f64,
    dec_bins: usize,
    // total_gsc_bins: usize,
    // max_ra_bins: usize,
    master_index: Vec<GscBinIndex>,
}

#[derive(Debug)]
struct GscBinIndex {
    // declination: f64,
    start_bin: usize,
    num_bins: usize,
}

impl GscBinning {
    pub fn new64() -> Self {
        // bin size is 1/64 of a degree
        // number of dec bins is 180 / bin_size
        // total bins is empirical
        Self::new_generic(0.015625, 11520, 168966386)
    }

    fn new_generic(bin_size: f64, dec_bins: usize, total_gsc_bins: usize) -> Self {
        let mut master_index = Vec::with_capacity(dec_bins);
        let mut ra_sum = 0;
        let mut max_ra_bins = 0;

        for i_bin in 0..dec_bins {
            let declination = i_bin as f64 * bin_size - 90.0;
            let num_ra_bins =
                (360. / bin_size * f64::cos((declination + bin_size / 2.) * D2R)) as usize;

            master_index.push(GscBinIndex {
                // declination,
                start_bin: ra_sum,
                num_bins: num_ra_bins,
            });

            max_ra_bins = usize::max(max_ra_bins, num_ra_bins);
            ra_sum += num_ra_bins;
        }

        if ra_sum != total_gsc_bins {
            panic!("consistency error in GSC bin definition");
        }

        GscBinning {
            bin_size,
            dec_bins,
            // total_gsc_bins,
            // max_ra_bins,
            master_index,
        }
    }

    /// Given a declination in degrees, get the declination bin number for this
    /// binning. The result is between 0 and `dec_bins`.
    pub fn get_dec_bin(&self, dec: f64) -> usize {
        if dec < -90. || dec > 90. {
            panic!("illegal declination {dec}");
        }

        let bin = ((dec + 90.) / self.bin_size) as usize;

        if bin == self.dec_bins {
            bin - 1
        } else {
            bin
        }
    }

    /// Given a declination bin number (between 0 and `dec_bins`) and an RA in
    /// degrees, get the "total" bin number associated with the RA. The result
    /// is between 0 and `total_gsc_bins`.
    pub fn get_total_bin(&self, dec_bin: usize, mut ra_deg: f64) -> usize {
        while ra_deg < 0. {
            ra_deg += 360.;
        }

        while ra_deg >= 360. {
            ra_deg -= 360.;
        }

        let bin_info = &self.master_index[dec_bin];
        let mut delta_bin = (ra_deg * bin_info.num_bins as f64 / 360.) as usize;

        if delta_bin >= bin_info.num_bins {
            delta_bin = bin_info.num_bins - 1;
        }

        bin_info.start_bin + delta_bin
    }
}
