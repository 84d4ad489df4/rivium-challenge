//! LinkSpool desktop entry point.

use std::ffi::OsString;

fn main() {
    let code = rivium_host::process::run_args::<linkspool::app::App>(default_root());
    std::process::exit(i32::from(code.exit()));
}

/// Use the current directory as the application root unless one was supplied.
///
/// Rivium defaults the root to the executable directory, which makes
/// `cargo run` look for configuration under `target/debug`. LinkSpool is a
/// local daemon, so the directory the operator runs it from is the useful
/// default for its configuration, database and inbox.
fn default_root() -> Vec<OsString> {
    let mut args: Vec<OsString> = std::env::args_os().skip(1).collect();
    let explicit = args.iter().any(|arg| {
        arg == "--root" || arg.to_string_lossy().starts_with("--root=")
    }) || std::env::var_os("LINKSPOOL_ROOT").is_some();
    if !explicit {
        args.splice(0..0, [OsString::from("--root"), OsString::from(".")]);
    }
    args
}
