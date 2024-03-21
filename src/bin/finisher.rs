// finisher.rs

use log::{error, info};
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;
use tacc_sync::{
    boolify, clean_up_and_exit, find_json_files_in_directory, load_request_from_file,
    load_work_from_file, move_to_outbox, TaccSyncRequest
};

/// the process exit code indicating successful exit
const EXIT_SUCCESS: i32 = 0;

/// the version of the package being compiled
const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
    // initialize logging
    env_logger::init();
    info!("tacc-sync v{} - finisher starting", VERSION);

    // load configuration from environment
    let globus_dir = std::env::var("GLOBUS_DIR").expect("GLOBUS_DIR environment variable not set");
    let hpss_dir = std::env::var("HPSS_DIR").expect("HPSS_DIR environment variable not set");
    let inbox_dir = std::env::var("INBOX_DIR").expect("INBOX_DIR environment variable not set");
    let outbox_dir = std::env::var("OUTBOX_DIR").expect("OUTBOX_DIR environment variable not set");
    let pid_path = std::env::var("PID_PATH").expect("PID_PATH environment variable not set");
    let quarantine_dir = std::env::var("QUARANTINE_DIR").expect("QUARANTINE_DIR environment variable not set");
    let reaper_dir = std::env::var("REAPER_DIR").expect("REAPER_DIR environment variable not set");
    let run_once_and_die = std::env::var("RUN_ONCE_AND_DIE").expect("RUN_ONCE_AND_DIE environment variable not set");
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
            // if we are able to load the sync request from the file
            if let Ok(request) = load_request_from_file(json_file) {
                // process the sync request
                process_sync_request(
                    json_file,
                    &PathBuf::from(&outbox_dir),
                    &request,
                    &PathBuf::from(&hpss_dir),
                    &PathBuf::from(&globus_dir),
                    &PathBuf::from(&reaper_dir),
                );
            }
            // we weren't able to load the sync request
            else {
                error!("Unable to load TaccSyncRequest: {}", json_file_str);
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

fn process_sync_request(
    json_file: &PathBuf,        // the request file being processed
    outbox_dir: &PathBuf,       // the outbox where the request will go if no work is associated with the request
    request: &TaccSyncRequest,  // the request object
    hpss_dir: &PathBuf,         // the hpss_dir where work may be found
    globus_dir: &PathBuf,       // the globus_dir where work may be found
    reaper_dir: &PathBuf        // the reaper_dir where work may be found
) {
    // determine the request_id
    info!("Checking for work with request_id = {}", request.request_id);
    // create a vector of the directories we intend to check
    let check_dirs = vec![hpss_dir, globus_dir, reaper_dir];
    // for each directory that we need to check for work
    for check_dir in check_dirs {
        // log about the directory we're checking
        info!("Checking for work in directory {}", check_dir.display());
        // find all the work files in the directory
        let dir_path = check_dir.to_string_lossy().to_string();
        let work_files = find_json_files_in_directory(&dir_path);
        // for each work file
        for work_file in work_files {
            // if we are able to load the work
            if let Ok(work) = load_work_from_file(&work_file) {
                // if the request id from the work matches the request id from the request
                if work.request_id == request.request_id {
                    // we're done with this request; work is in-flight, so this request
                    // is NOT finished, and it is NOT ready to go to the finished directory
                    info!("Work {} has request_id {}", work_file.display(), work.request_id);
                    info!("Will NOT move the request to {}", outbox_dir.display());
                    return
                }
            }
            else {
                // we were unable to load the work; this may have happened because another
                // component was processing the work, finished it, and moved it downstream
                // before we had a chance to load it. to cover this case, we'll log an error
                // but take no specific action. we'll check this request on another cycle
                error!("Unable to load work {}", work_file.display());
                info!("Will NOT move the request to {}", outbox_dir.display());
                return
            }
        }
    }
    // we've checked every work unit in all of the directories
    // none of them contain the request id of this request
    // the request IS finished, and IS ready to move to the finished directory
    info!("Work directories have been exhausted. Request {} is completely finished.", request.request_id);
    move_to_outbox(json_file, &PathBuf::from(&outbox_dir));
}
