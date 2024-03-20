// finisher.rs

use log::{debug, error, info, Level, log_enabled, trace};

/// the version of the package being compiled
const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
    // initialize logging
    env_logger::init();
    info!("tacc-sync v{} - finisher starting", VERSION);
}
