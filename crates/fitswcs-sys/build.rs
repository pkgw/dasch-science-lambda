//! Based on rust-cfitsio

use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};

fn compile_cfitsio() -> PathBuf {
    use autotools::Config;

    let cfitsio_project_dir = PathBuf::from("ext/cfitsio");
    if !cfitsio_project_dir.exists() {
        panic!(
            "Expected to find cfitsio source directory {}",
            cfitsio_project_dir.display()
        );
    }

    // Translate rustc optimisation levels to things a C compiler can
    // understand. I don't know if all C compilers agree here, but it should
    // at least work for gcc.
    let opt_level = match std::env::var("OPT_LEVEL").as_ref().map(|o| o.as_str()) {
        Err(_) => panic!("Something wrong with OPT_LEVEL"),
        // gcc doesn't handle 'z'. Just set it to 's', which also optimises
        // for size.
        Ok("z") => "s",
        Ok(o) => o,
    }
    .to_string();

    let opt_flag = format!("-O{opt_level}");

    Config::new("ext/cfitsio")
        .disable_shared()
        .enable_static()
        .disable("curl", None)
        .enable("reentrant", None)
        .cflag(opt_flag)
        .cflag("-fPIE")
        .insource(true)
        .build()
}

fn compile_wcslib(cfitsio_prefix: &Path) -> PathBuf {
    use autotools::Config;

    let project_dir = PathBuf::from("ext/wcslib");
    if !project_dir.exists() {
        panic!(
            "Expected to find cfitsio source directory {}",
            project_dir.display()
        );
    }

    // Translate rustc optimisation levels to things a C compiler can
    // understand. I don't know if all C compilers agree here, but it should
    // at least work for gcc.
    let opt_level = match std::env::var("OPT_LEVEL").as_ref().map(|o| o.as_str()) {
        Err(_) => panic!("Something wrong with OPT_LEVEL"),
        // gcc doesn't handle 'z'. Just set it to 's', which also optimises
        // for size.
        Ok("z") => "s",
        Ok(o) => o,
    }
    .to_string();

    let opt_flag = format!("-O{opt_level}");

    let mut cfinc = cfitsio_prefix.to_owned();
    cfinc.push("include");

    let mut cflib = cfitsio_prefix.to_owned();
    cflib.push("lib");

    Config::new("ext/wcslib")
        .disable_shared()
        .disable("flex", None)
        .disable("fortran", None)
        .disable("utils", None)
        .without("pgplot", None)
        .with(OsStr::new("cfitsiolib"), Some(cflib.as_os_str()))
        .with(OsStr::new("cfitsioinc"), Some(cfinc.as_os_str()))
        .cflag(opt_flag)
        .cflag("-fPIE")
        .insource(true)
        .build()
}

fn main() {
    let cfitsio_prefix = compile_cfitsio();
    let wcslib_prefix = compile_wcslib(&cfitsio_prefix);

    let mut p = cfitsio_prefix.clone();
    p.push("lib");
    println!("cargo:rustc-link-search=native={}", p.display());

    let mut p = wcslib_prefix.clone();
    p.push("lib");
    println!("cargo:rustc-link-search=native={}", p.display());

    // Note: link flags are issues in the order we emit these, so if `liba`
    // depends on `libb`, `liba` should be printed first.
    println!("cargo:rustc-link-lib=static=wcs");
    println!("cargo:rustc-link-lib=static=cfitsio");
    println!("cargo:rustc-link-lib=static=z");
}
