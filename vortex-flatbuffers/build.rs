use std::env;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;
use walkdir::WalkDir;

fn main() {
    let buildrs_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .canonicalize()
        .expect("Failed to canonicalize CARGO_MANIFEST_DIR");
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap())
        .canonicalize()
        .expect("Failed to canonicalize OUT_DIR");
    let _root_dir = buildrs_dir
        .join("../")
        .canonicalize()
        .expect("Failed to canonicalize root dir");
    let flatbuffers_dir = buildrs_dir.join("flatbuffers");

    rerun_if_changed(&buildrs_dir.join("build.rs"));
    WalkDir::new(&flatbuffers_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .for_each(|e| rerun_if_changed(e.path()));

    let exit = Command::new("flatc")
        .arg("--rust")
        .arg("-o")
        .arg(out_dir.join("flatbuffers"))
        .args(
            WalkDir::new(&flatbuffers_dir)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension() == Some(OsStr::new("fbs")))
                .map(|d| d.path().to_path_buf())
                .map(|d| {
                    rerun_if_changed(d.as_path());
                    d
                }),
        )
        .status()
        .expect("Failed to execute flatc")
        .success();
    assert!(exit, "flatc failed");
}

fn rerun_if_changed(path: &Path) {
    println!(
        "cargo:rerun-if-changed={}",
        path.canonicalize()
            .unwrap_or_else(|_| panic!("failed to canonicalize {}", path.to_str().unwrap()))
            .to_str()
            .unwrap()
    );
}
