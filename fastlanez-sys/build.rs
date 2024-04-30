use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use walkdir::WalkDir;

#[allow(clippy::panic)]
fn main() {
    let buildrs_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("Missing CARGO_MANIFEST_DIR"))
            .canonicalize()
            .expect("Failed to canonicalize CARGO_MANIFEST_DIR");
    let root_dir = buildrs_dir
        .join("../")
        .canonicalize()
        .expect("Failed to canonicalize root dir");
    let fastlanez_dir = root_dir.join("deps/fastlanez");

    // Tell cargo to tell rustc to link fastlanez
    println!(
        "cargo:rustc-link-search={}",
        fastlanez_dir
            .join("zig-out/lib")
            .to_str()
            .expect("invalid zig-out/lib path")
    );
    println!("cargo:rustc-link-lib=fastlanez");

    rerun_if_changed(&buildrs_dir.join("build.rs"));
    WalkDir::new(fastlanez_dir.join("src"))
        .into_iter()
        .filter_map(|e| e.ok())
        .for_each(|e| rerun_if_changed(e.path()));

    let zig_opt = get_zig_opt();
    println!("cargo:info=invoking `zig build` with {}", zig_opt);
    if !Command::new("zig")
        .args(["build", "lib"])
        .arg(zig_opt)
        .args(["--summary", "all"])
        .current_dir(fastlanez_dir.clone())
        .spawn()
        .expect("Could not invoke `zig build`")
        .wait()
        .unwrap_or_else(|e| panic!("failed to invoke `zig build`: {}", e))
        .success()
    {
        // Panic if the command was not successful.
        panic!(
            "failed to successfully invoke `zig build` in {}",
            root_dir.to_str().expect("invalid root_dir path")
        );
    }

    let bindings = bindgen::Builder::default()
        .header(
            fastlanez_dir
                .join("zig-out/include/fastlanez.h")
                .to_str()
                .expect("invalid zig-out/include/fastlanez.h path"),
        )
        .clang_args(&[
            get_zig_include().as_ref(),
            "-DZIG_TARGET_MAX_INT_ALIGNMENT=16",
        ])
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .allowlist_item("fl_.*")
        .generate()
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").expect("Missing OUT_DIR"));
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

#[allow(clippy::panic)]
fn rerun_if_changed(path: &Path) {
    println!(
        "cargo:rerun-if-changed={}",
        path.canonicalize()
            .unwrap_or_else(|_| panic!(
                "failed to canonicalize {}",
                path.to_str().expect("invalid path")
            ))
            .to_str()
            .expect("invalid path")
    );
}

fn get_zig_opt() -> &'static str {
    let profile_env = env::var("PROFILE").expect("Missing PROFILE");
    let opt_level_zero = env::var("OPT_LEVEL").expect("Missing OPT_LEVEL") == "0";

    // based on https://doc.rust-lang.org/cargo/reference/environment-variables.html
    //
    // confusingly, the PROFILE env var will be either "debug" or "release" depending on whether the cargo profile
    // derives from the "dev" or "release" profile, respectively. *facepalm*
    // so `cargo build` and `cargo test` will be "debug"; `cargo build --release` and `cargo bench` will be "release"
    //
    // we also check whether debug_assertions are enabled (to pick a sane value for custom profiles)
    if profile_env == "debug" || cfg!(debug_assertions) {
        "-Doptimize=Debug"
    } else if profile_env == "release" || !opt_level_zero {
        "-Doptimize=ReleaseSmall"
    } else {
        // we're in a custom profile, the opt_level is 0, but debug assertions aren't enabled
        // pretty weird case, let's default to debug
        println!(
            "cargo:warning=unrecognized cargo profile {}, defaulting to `zig build -Doptimize=Debug`", profile_env
        );
        "-Doptimize=Debug"
    }
}

fn get_zig_include() -> String {
    String::from_utf8(
        Command::new("bash")
            .arg("-c")
            .arg("zig env | grep lib_dir | awk -F'\"' '{print \"-I\"$4}'")
            .stdout(Stdio::piped())
            .output()
            .expect("Failed to execute command")
            .stdout,
    )
    .expect("Failed to convert command output to string")
    .trim_end()
    .to_string()
}
