use anyhow::{bail, Result};
use fitswcs_sys::wcslib;
use libc::{c_char, c_int};
use ndarray::{Array, Ix3};

#[derive(Debug)]
pub struct Wcs {
    handle: wcslib::WcsPrm,
}

/// Our error handling is super lame.
macro_rules! try_wcslib {
    ($status:expr) => {{
        let s = $status;

        if s != 0 {
            bail!("wcslib error code {}", s);
        }
    }};
}

impl Wcs {
    /// Initialize WCS from FITS headers, based on a raw pointer.
    pub unsafe fn new_raw(header: *const c_char, nkeys: c_int) -> Result<Self> {
        let mut handle: wcslib::WcsPrm = std::ptr::null_mut();
        let mut nreject: c_int = 0;
        let mut nwcs: c_int = 0;

        try_wcslib!(unsafe {
            wcslib::wcspih(
                header,
                nkeys,
                wcslib::WCSHDR_ALL,
                0,
                &mut nreject,
                &mut nwcs,
                &mut handle,
            )
        });

        println!("** wcs {nreject} {nwcs}");

        Ok(Wcs { handle })
    }

    /// Sample world coordinates on a grid of pixel indices.
    pub fn sample_world_square(&mut self, size: usize) -> Result<Array<f64, Ix3>> {
        const NELEM: c_int = 2;

        // Pixel coordinate array to be fed into wcslib: we can treat it as a
        // NxNx2 array of 1-based X and Y coordinates.
        let pixel = Array::from_shape_fn((size, size, 2), |(i, j, k)| {
            if k == 0 {
                j as f64 + 1.
            } else {
                i as f64 + 1.
            }
        });

        let mut image = Array::<f64, _>::uninit(pixel.dim());
        let mut phi = Array::<f64, _>::uninit(pixel.dim());
        let mut theta = Array::<f64, _>::uninit(pixel.dim());
        let mut world = Array::<f64, _>::uninit(pixel.dim());
        let mut status = Array::<c_int, _>::uninit((size, size));

        try_wcslib!(unsafe {
            wcslib::wcsp2s(
                self.handle,
                (size * size) as c_int,
                NELEM,
                pixel.as_ptr(),
                image.as_mut_ptr() as *mut _,
                phi.as_mut_ptr() as *mut _,
                theta.as_mut_ptr() as *mut _,
                world.as_mut_ptr() as *mut _,
                status.as_mut_ptr() as *mut _,
            )
        });

        // Let's just ignore any problems.

        Ok(unsafe { world.assume_init() })
    }

    /// Convert world coordinates to pixel coordinates. The returned coordinates
    /// are 0-based.
    ///
    /// As usual here, we hardcode for our specific use case.
    pub fn world_to_pixel(&mut self, world: Array<f64, Ix3>) -> Result<Array<f64, Ix3>> {
        let ncoord = world.shape()[0] * world.shape()[1];
        const NELEM: c_int = 2;

        let mut phi = Array::<f64, _>::uninit(world.dim());
        let mut theta = Array::<f64, _>::uninit(world.dim());
        let mut image = Array::<f64, _>::uninit(world.dim());
        let mut pixel = Array::<f64, _>::uninit(world.dim());
        let mut status = Array::<c_int, _>::uninit(ncoord);

        try_wcslib!(unsafe {
            wcslib::wcss2p(
                self.handle,
                ncoord as c_int,
                NELEM,
                world.as_ptr(),
                phi.as_mut_ptr() as *mut _,
                theta.as_mut_ptr() as *mut _,
                image.as_mut_ptr() as *mut _,
                pixel.as_mut_ptr() as *mut _,
                status.as_mut_ptr() as *mut _,
            )
        });

        let mut pixel = unsafe { pixel.assume_init() };

        // Convert to 0-based pixel indices.
        pixel -= 1.;

        Ok(pixel)
    }

    /// Dumb utility. We should use generics better.
    pub fn world_to_pixel_scalar(&mut self, ra_deg: f64, dec_deg: f64) -> Result<(f64, f64)> {
        let mut world = Array::zeros((1, 1, 2));
        world[(0, 0, 0)] = ra_deg;
        world[(0, 0, 1)] = dec_deg;
        let pixel = self.world_to_pixel(world)?;
        Ok((pixel[(0, 0, 0)], pixel[(0, 0, 1)]))
    }

    /// Dumb utility. We should use generics better.
    ///
    /// We use 0-based pixel indexes.
    pub fn pixel_to_world_scalar(&mut self, x: f64, y: f64) -> Result<(f64, f64)> {
        const NELEM: c_int = 2;

        let mut pixel = Array::zeros(2);
        pixel[0] = x + 1.;
        pixel[1] = x + 1.;

        let mut image = Array::<f64, _>::uninit(pixel.dim());
        let mut phi = Array::<f64, _>::uninit(pixel.dim());
        let mut theta = Array::<f64, _>::uninit(pixel.dim());
        let mut world = Array::<f64, _>::uninit(pixel.dim());
        let mut status = Array::<c_int, _>::uninit(1);

        try_wcslib!(unsafe {
            wcslib::wcsp2s(
                self.handle,
                1,
                NELEM,
                pixel.as_ptr(),
                image.as_mut_ptr() as *mut _,
                phi.as_mut_ptr() as *mut _,
                theta.as_mut_ptr() as *mut _,
                world.as_mut_ptr() as *mut _,
                status.as_mut_ptr() as *mut _,
            )
        });

        // Let's just ignore any problems.

        let world = unsafe { world.assume_init() };
        Ok((world[0], world[1]))
    }
}

impl Drop for Wcs {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe {
                wcslib::wcsfree(self.handle);
            }
            self.handle = std::ptr::null_mut();
        }
    }
}
