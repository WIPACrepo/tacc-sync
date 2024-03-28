// reaper.rs

use log::{error, info};
use std::fs;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;
use tacc_sync::{
    boolify, clean_up_and_exit, find_json_files_in_directory,
    load_work_from_file, move_to_outbox
};

/// the process exit code indicating successful exit
const EXIT_SUCCESS: i32 = 0;

/// the version of the package being compiled
const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
    // initialize logging
    env_logger::init();
    info!("tacc-sync v{} - reaper starting", VERSION);

    // load configuration from environment
    let inbox_dir = std::env::var("INBOX_DIR").expect("INBOX_DIR environment variable not set");
    let outbox_dir = std::env::var("OUTBOX_DIR").expect("OUTBOX_DIR environment variable not set");
    let pid_path = std::env::var("PID_PATH").expect("PID_PATH environment variable not set");
    let quarantine_dir = std::env::var("QUARANTINE_DIR").expect("QUARANTINE_DIR environment variable not set");
    let run_once_and_die = std::env::var("RUN_ONCE_AND_DIE").expect("RUN_ONCE_AND_DIE environment variable not set");
    let transfer_dir = std::env::var("TRANSFER_DIR").expect("TRANSFER_DIR environment variable not set");
    let work_sleep_seconds = std::env::var("WORK_SLEEP_SECONDS").expect("WORK_SLEEP_SECONDS environment variable not set");

    let run_once = boolify(&run_once_and_die);
    let sleep_seconds = work_sleep_seconds.parse::<u64>().expect("WORK_SLEEP_SECONDS environment variable must be an integer");

    // run the main loop
    loop {
        info!("Starting work cycle");

        // search the inbox for work to do
        info!("Checking for work in inbox directory: {}", inbox_dir);
        let json_files = find_json_files_in_directory(&inbox_dir);
        let num_files = json_files.len();

        // for each unit of work
        info!("Processing {} work units", num_files);
        for (index, json_file) in json_files.iter().enumerate() {
            let json_file_str = json_file.as_path().display();
            info!("Processing {}/{}: {}", index+1, num_files, json_file_str);
            // if we are able to load the work from the file
            if let Ok(work) = load_work_from_file(json_file) {
                // remove the directory in the transfer buffer
                info!("Deleting files for {}: {} ({} files - {} bytes)", work.work_id, work.tape, work.files.len(), work.size);
                let transfer_pb = PathBuf::from(&transfer_dir);
                let hpss_out_dir = transfer_pb.join(format!("{}", work.work_id));
                info!("Deleting transfer buffer directory: {}", hpss_out_dir.display());
                fs::remove_dir_all(&hpss_out_dir).expect("Unable to remove output directory in transfer buffer");
                // send the work to the finished directory
                move_to_outbox(json_file, &PathBuf::from(&outbox_dir));
            }
            // we weren't able to load the work
            else {
                error!("Unable to load TaccSyncWork: {}", json_file_str);
                move_to_outbox(json_file, &PathBuf::from(&quarantine_dir));
            }
        }

        // if this was a one-shot adventure
        if run_once {
            info!("RUN_ONCE_AND_DIE: {} -- finisher now ending", run_once_and_die);
            clean_up_and_exit(&pid_path, EXIT_SUCCESS);
        }

        // otherwise, sleep until we need to wake up again
        info!("Sleeping for {} seconds...", sleep_seconds);
        sleep(Duration::from_secs(sleep_seconds));
    }
}
