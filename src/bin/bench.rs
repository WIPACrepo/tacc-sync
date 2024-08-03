// bench.rs

// use anyhow::Result;
// use log::{debug, error, info};
use log::info;
// use std::fs::{self, File};
// use std::io::{BufWriter, Write};
use std::path::PathBuf;
// use std::process::Command;
// use std::thread::sleep;
// use std::time::Duration;
// use tacc_sync::{
//     boolify, clean_up_and_exit, find_json_files_in_directory,
//     load_work_from_file, move_to_outbox, TaccSyncWork
// };
use tacc_sync::load_work_from_file;
// use uuid::Uuid;
// use walkdir::WalkDir;

// /// the process exit code indicating successful exit
// const EXIT_SUCCESS: i32 = 0;

// /// the constant meaning unlimited space is available
// const UNLIMITED_SPACE: u64 = 0;

/// the version of the package being compiled
const VERSION: &'static str = env!("CARGO_PKG_VERSION");

// fn main() {
//     // initialize logging
//     env_logger::init();
//     info!("tacc-sync v{} - retriever starting", VERSION);

//     // load configuration from environment
//     let hpss_base_path = std::env::var("HPSS_BASE_PATH").expect("HPSS_BASE_PATH environment variable not set");
//     let inbox_dir = std::env::var("INBOX_DIR").expect("INBOX_DIR environment variable not set");
//     let outbox_dir = std::env::var("OUTBOX_DIR").expect("OUTBOX_DIR environment variable not set");
//     let pid_path = std::env::var("PID_PATH").expect("PID_PATH environment variable not set");
//     let quarantine_dir = std::env::var("QUARANTINE_DIR").expect("QUARANTINE_DIR environment variable not set");
//     let run_once_and_die = std::env::var("RUN_ONCE_AND_DIE").expect("RUN_ONCE_AND_DIE environment variable not set");
//     let semaphore_dir = std::env::var("SEMAPHORE_DIR").expect("SEMAPHORE_DIR environment variable not set");
//     let transfer_dir = std::env::var("TRANSFER_DIR").expect("TRANSFER_DIR environment variable not set");
//     let transfer_quota = std::env::var("TRANSFER_QUOTA").expect("TRANSFER_QUOTA environment variable not set");
//     let work_dir = std::env::var("WORK_DIR").expect("WORK_DIR environment variable not set");
//     let work_sleep_seconds = std::env::var("WORK_SLEEP_SECONDS").expect("WORK_SLEEP_SECONDS environment variable not set");

//     let space_allowed = transfer_quota.parse::<u64>().expect("TRANSFER_QUOTA environment variable must be an integer");
//     let run_once = boolify(&run_once_and_die);
//     let sleep_seconds = work_sleep_seconds.parse::<u64>().expect("WORK_SLEEP_SECONDS environment variable must be an integer");

//     // run the main loop
//     loop {
//         info!("Starting work cycle");

//         // search the inbox for work to do
//         info!("Checking for work in inbox directory: {}", inbox_dir);
//         let json_files = find_json_files_in_directory(&inbox_dir);
//         let num_files = json_files.len();

//         // for each unit of work
//         info!("Processing {} work units", num_files);
//         for (index, json_file) in json_files.iter().enumerate() {
//             let json_file_str = json_file.as_path().display();
//             info!("Processing {}/{}: {}", index+1, num_files, json_file_str);

//             // if we are able to load the work from the file
//             if let Ok(work) = load_work_from_file(json_file) {
//                 // if we have to worry about disk quota
//                 if space_allowed != UNLIMITED_SPACE {
//                     // determine if there is enough space in the transfer buffer
//                     let space_required = calculate_directory_size(&PathBuf::from(&transfer_dir)) + work.size;
//                     if space_required > space_allowed {
//                         info!("Transfer buffer is full: {} bytes needed > {} bytes allowed", space_required, space_allowed);
//                         info!("Will STOP processing work until the next cycle.");
//                         break;
//                     }
//                 }

//                 // move the work unit to the work directory
//                 let work_file = move_to_outbox(json_file, &PathBuf::from(&work_dir));
//                 // process the work
//                 match process_work(&work, &hpss_base_path, &PathBuf::from(&transfer_dir), &PathBuf::from(&semaphore_dir)) {
//                     Ok(_) => {
//                         move_to_outbox(&work_file, &PathBuf::from(&outbox_dir));
//                     },
//                     Err(e) => {
//                         error!("Error processing work: {}", e);
//                         move_to_outbox(&work_file, &PathBuf::from(&quarantine_dir));
//                     }
//                 }
//             }
//             // we weren't able to load the sync request
//             else {
//                 error!("Unable to load TaccSyncWork: {}", json_file_str);
//                 move_to_outbox(json_file, &PathBuf::from(&quarantine_dir));
//             }
//         }

//         // if this was a one-shot adventure
//         if run_once {
//             info!("RUN_ONCE_AND_DIE: {} -- retriever now ending", run_once_and_die);
//             clean_up_and_exit(&pid_path, EXIT_SUCCESS);
//         }

//         // otherwise, sleep until we need to wake up again
//         info!("Sleeping for {} seconds...", sleep_seconds);
//         sleep(Duration::from_secs(sleep_seconds));
//     }
// }

// fn process_work(
//     work: &TaccSyncWork,
//     hpss_base_path: &str,
//     transfer_dir: &PathBuf,
//     semaphore_dir: &PathBuf
// ) -> Result<()> {
//     // log about what we're processing
//     info!("Retrieving files for {}: {} ({} files - {} bytes)", work.work_id, work.tape, work.files.len(), work.size);

//     // create a temporary file we can feed to hsi
//     let file_name = Uuid::new_v4().to_string();
//     let hsi_batch_path = semaphore_dir.join(file_name);
//     let hsi_batch_file = File::create(&hsi_batch_path).expect("Unable to create hsi batch temporary file");
//     info!("hsi batch file: {}", hsi_batch_path.display());
//     let mut writer = BufWriter::new(hsi_batch_file);

//     // we batch the hsi copy commands into the file
//     for file in &work.files {
//         // determine where the file lives in HPSS
//         let hpss_path = &file.hpss_path;
//         // determine where we want the disk copy to go
//         let start_index = hpss_base_path.len();
//         let data_warehouse_path = &hpss_path[start_index + 1..];
//         let output_path = transfer_dir.join(data_warehouse_path);
//         // create the directory in the transfer buffer
//         let output_parent = output_path.parent().expect("Unable to determine disk location for HPSS file");
//         info!("Creating transfer buffer directory: {}", output_parent.display());
//         fs::create_dir_all(&output_parent).expect("Unable to create output directory in transfer buffer");
//         // write the command to the hsi batch file
//         // get      get a file from hpss
//         // -C       purge the file from hpss disk cache; we'll only read it just the once to put it on icecube disk
//         // -P       preserve timestamps as recorded in hpss
//         // {}       the place where we want to put the file on disk
//         // :        gotta love good ol hsi
//         // {}       the place where the file is stored in hpss
//         writeln!(writer, "get -C -P {} : {}", output_path.display(), hpss_path).expect("Unable to write to hsi batch temporary file");
//     }
//     writer.flush().expect("Unable to close hsi batch temporary file");

//     // run the hsi command, feeding it the batch file
//     info!("Running hsi command: hsi -P in {}", hsi_batch_path.display());
//     let output = Command::new("hsi")
//         .arg("-P")
//         .arg("in")
//         .arg(&hsi_batch_path)
//         .output()
//         .expect("Unable to execute hsi batch copy for work unit");

//     // remove our temporary file
//     info!("Removing hsi batch file: {}", hsi_batch_path.display());
//     std::fs::remove_file(hsi_batch_path).expect("Unable to delete hsi batch temporary file");

//     // check the output to see that everything succeeded?
//     let stdout = String::from_utf8(output.stdout).expect("hsi output does not conform to utf8 encoding");
//     debug!("{}", stdout);

//     // tell the caller that we succeeded
//     Ok(())
// }

// /// Calculate the total size of files in a directory and its subdirectories.
// /// 
// /// # Arguments
// /// 
// /// * `root_path` - The path to the directory whose total size should be calculated.
// /// 
// /// # Returns
// /// 
// /// The total size of all files in bytes as a `u64`.
// fn calculate_directory_size(root_path: &PathBuf) -> u64 {
//     WalkDir::new(root_path)
//         .into_iter()
//         .filter_map(|e| e.ok()) // Filter out any Errs and unwrap
//         .filter(|e| e.file_type().is_file()) // Consider only files
//         .map(|e| e.path().to_owned()) // Convert DirEntry to Path
//         .filter_map(|path| fs::metadata(path).ok()) // Get metadata, filter out errors
//         .filter_map(|metadata| metadata.len().checked_add(0)) // Extract file size, ignore files we can't get size for
//         .sum() // Sum up all file sizes
// }



fn main() {
    // initialize logging
    env_logger::init();
    info!("tacc-sync v{} - bench starting", VERSION);

    let json_file = PathBuf::from("/tmp/retriever/0001fb40-cd0f-41f1-aee0-81baf6b63576.json");

    let result = load_work_from_file(&json_file);

    match result {
        Ok(x) => {
            info!("Got {:?}", x);
        },
        Err(e) => {
            info!("Error: {}", e);
        }
    }
}
