// verifier.rs

use anyhow::Result;
use log::{error, info};
use std::path::PathBuf;
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;
use tacc_sync::{
    boolify, clean_up_and_exit, find_json_files_in_directory,
    load_work_from_file, move_to_outbox, TaccSyncWork
};

/// the process exit code indicating successful exit
const EXIT_SUCCESS: i32 = 0;

/// the version of the package being compiled
const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
    // initialize logging
    env_logger::init();
    info!("tacc-sync v{} - verifier starting", VERSION);

    // load configuration from environment
    let hpss_base_path = std::env::var("HPSS_BASE_PATH").expect("HPSS_BASE_PATH environment variable not set");
    let inbox_dir = std::env::var("INBOX_DIR").expect("INBOX_DIR environment variable not set");
    let outbox_dir = std::env::var("OUTBOX_DIR").expect("OUTBOX_DIR environment variable not set");
    let pid_path = std::env::var("PID_PATH").expect("PID_PATH environment variable not set");
    let quarantine_dir = std::env::var("QUARANTINE_DIR").expect("QUARANTINE_DIR environment variable not set");
    let run_once_and_die = std::env::var("RUN_ONCE_AND_DIE").expect("RUN_ONCE_AND_DIE environment variable not set");
    let transfer_dir = std::env::var("TRANSFER_DIR").expect("TRANSFER_DIR environment variable not set");
    let work_dir = std::env::var("WORK_DIR").expect("WORK_DIR environment variable not set");
    let work_sleep_seconds = std::env::var("WORK_SLEEP_SECONDS").expect("WORK_SLEEP_SECONDS environment variable not set");

    // let space_allowed = transfer_quota.parse::<u64>().expect("TRANSFER_QUOTA environment variable must be an integer");
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
                // move the work unit to the work directory
                let work_file = move_to_outbox(json_file, &PathBuf::from(&work_dir));
                // process the work
                match process_work(&work, &hpss_base_path, &PathBuf::from(&transfer_dir)) {
                    Ok(_) => {
                        move_to_outbox(&work_file, &PathBuf::from(&outbox_dir));
                    },
                    Err(e) => {
                        error!("Error processing work: {}", e);
                        move_to_outbox(&work_file, &PathBuf::from(&quarantine_dir));
                    }
                }
            }
            // we weren't able to load the sync request
            else {
                error!("Unable to load TaccSyncWork: {}", json_file_str);
                move_to_outbox(json_file, &PathBuf::from(&quarantine_dir));
            }
        }

        // if this was a one-shot adventure
        if run_once {
            info!("RUN_ONCE_AND_DIE: {} -- verifier now ending", run_once_and_die);
            clean_up_and_exit(&pid_path, EXIT_SUCCESS);
        }

        // otherwise, sleep until we need to wake up again
        info!("Sleeping for {} seconds...", sleep_seconds);
        sleep(Duration::from_secs(sleep_seconds));
    }
}

fn process_work(
    work: &TaccSyncWork,
    hpss_base_path: &str,
    transfer_dir: &PathBuf,
) -> Result<()> {
    // log about what we're processing
    info!("Verifying files for {}: {} ({} files - {} bytes)", work.work_id, work.tape, work.files.len(), work.size);

    // for each file in the work unit
    let mut index = 0;
    for file in &work.files {
        // log about what we're processing
        index = index + 1;
        info!("Processing {}/{}: {}", index, work.files.len(), file.file_name);
        // determine the sha512 checksum of the file
        let checksum = file.checksum.as_ref().ok_or(anyhow::anyhow!("missing checksums"))?;
        let sha512 = &checksum.sha512;
        // determine where the file lives in HPSS
        let hpss_path = &file.hpss_path;
        // determine where the file lives on disk
        let start_index = hpss_base_path.len();
        let data_warehouse_path = &hpss_path[start_index + 1..];
        let output_path = transfer_dir.join(data_warehouse_path);
        // run a sha512sum command to verify the checksum of the file
        let result = verify_checksum(output_path, sha512)?;
        if !result {
            return Err(anyhow::anyhow!("Checksum did not match!"));
        }
    }

    // we processed every file in the work unit and verified all the checksums
    info!("All files verified with correct checksums.");
    Ok(())
}

fn verify_checksum(output_path: PathBuf, expected_sha512: &str) -> Result<bool> {
    // run the sha512sum command to calculate the file's checksum
    info!("Running command: sha512sum {}", output_path.display());
    let output = Command::new("sha512sum")
        .arg(output_path)
        .output()?;

    // output format is "checksum  filename", so we split and take the first part
    let checksum_output = String::from_utf8(output.stdout)?;
    let calculated_sha512 = checksum_output.split_whitespace().next().unwrap_or("");

    // compare the calculated checksum with the expected checksum
    if calculated_sha512 != expected_sha512 {
        error!("Checksum mismatch. Expected:{} Found:{}", expected_sha512, calculated_sha512);
    }
    Ok(calculated_sha512 == expected_sha512)
}
