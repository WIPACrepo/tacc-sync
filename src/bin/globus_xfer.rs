// globus_xfer.rs

use anyhow::Result;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::{self, File};
use std::path::PathBuf;
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;
use tacc_sync::{
    boolify, clean_up_and_exit, find_json_files_in_directory,
    load_work_from_file, move_to_outbox, TaccSyncFile, TaccSyncWork
};
use uuid::Uuid;

/// the process exit code indicating successful exit
const EXIT_SUCCESS: i32 = 0;

/// the version of the package being compiled
const VERSION: &'static str = env!("CARGO_PKG_VERSION");

/// GlobusXferContext represents a bundle of configuration information;
/// instead of passing a dozen arguments, we gather them up into a
/// struct so we can pass it around more easily
pub struct GlobusXferContext {
    pub globus_dest_endpoint: String,
    pub globus_source_endpoint: String,
    pub hpss_base_path: String,
    pub inbox_dir: String,
    pub semaphore_dir: String,
    pub tacc_base_path: String,
    pub transfer_dir: String,
}

/// GlobusTask represents the task metdata returned by the Globus CLI
#[derive(Debug, Deserialize, Serialize)]
pub struct GlobusTask {
    /// the Globus task id for this transfer
    pub task_id: Uuid,
    /// One word indicating the progress of the task:
    /// "ACTIVE"
    ///     The task is in progress.
    /// "INACTIVE"
    ///     The task has been suspended and will not continue without intervention. Currently, only credential expiration will cause this state.
    /// "SUCCEEDED"
    ///     The task completed successfully.
    /// "FAILED"
    ///     The task or one of its subtasks failed, expired, or was canceled.
    pub status: String,
}

/// GlobusTransferCreated represents the result of the globus transfer command
#[derive(Debug, Deserialize, Serialize)]
pub struct TransferResult {
    #[serde(rename = "DATA_TYPE")]
    pub data_type: String,
    pub code: String,
    pub message: String,
    pub request_id: String,
    pub resource: String,
    pub submission_id: Uuid,
    pub task_id: Uuid,
}

/// TransferUpdate lets the process_file function report on the status of transfers
#[derive(Debug)]
struct TransferUpdate {
    pub finished: bool,
    pub updated: bool,
}

fn main() {
    // initialize logging
    env_logger::init();
    info!("tacc-sync v{} - globus_xfer starting", VERSION);

    // load transfer configuration from environment
    let globus_dest_endpoint = std::env::var("GLOBUS_DEST_ENDPOINT").expect("GLOBUS_DEST_ENDPOINT environment variable not set");
    let globus_source_endpoint = std::env::var("GLOBUS_SOURCE_ENDPOINT").expect("GLOBUS_SOURCE_ENDPOINT environment variable not set");
    let hpss_base_path = std::env::var("HPSS_BASE_PATH").expect("HPSS_BASE_PATH environment variable not set");
    let tacc_base_path = std::env::var("TACC_BASE_PATH").expect("TACC_BASE_PATH environment variable not set");

    // load configuration from environment
    let inbox_dir = std::env::var("INBOX_DIR").expect("INBOX_DIR environment variable not set");
    let outbox_dir = std::env::var("OUTBOX_DIR").expect("OUTBOX_DIR environment variable not set");
    let pid_path = std::env::var("PID_PATH").expect("PID_PATH environment variable not set");
    let quarantine_dir = std::env::var("QUARANTINE_DIR").expect("QUARANTINE_DIR environment variable not set");
    let run_once_and_die = std::env::var("RUN_ONCE_AND_DIE").expect("RUN_ONCE_AND_DIE environment variable not set");
    let semaphore_dir = std::env::var("SEMAPHORE_DIR").expect("SEMAPHORE_DIR environment variable not set");
    let transfer_dir = std::env::var("TRANSFER_DIR").expect("TRANSFER_DIR environment variable not set");
    let work_sleep_seconds = std::env::var("WORK_SLEEP_SECONDS").expect("WORK_SLEEP_SECONDS environment variable not set");

    let run_once = boolify(&run_once_and_die);
    let sleep_seconds = work_sleep_seconds.parse::<u64>().expect("WORK_SLEEP_SECONDS environment variable must be an integer");

    // create the context
    let context = GlobusXferContext {
        globus_dest_endpoint,
        globus_source_endpoint,
        hpss_base_path,
        tacc_base_path,
        inbox_dir: inbox_dir.clone(),
        semaphore_dir,
        transfer_dir,
    };

    // run the main loop
    loop {
        info!("Starting work cycle");

        // search the inbox for work to do
        info!("Checking for work in inbox directory: {}", &inbox_dir);
        let json_files = find_json_files_in_directory(&inbox_dir);
        let num_files = json_files.len();

        // for each unit of work
        info!("Processing {} work units", num_files);
        for (index, json_file) in json_files.iter().enumerate() {
            let json_file_str = json_file.as_path().display();
            info!("Processing {}/{}: {}", index+1, num_files, json_file_str);
            // if we are able to load the work from the file
            if let Ok(mut work) = load_work_from_file(json_file) {
                // process the work
                match process_work(&context, &mut work) {
                    Err(e) => {
                        error!("Error while processing work. Error was: {}", e);
                        move_to_outbox(json_file, &PathBuf::from(&quarantine_dir));
                    },
                    Ok(done) => {
                        if done {
                            info!("Transfers complete. Will move work unit to outbox.");
                            move_to_outbox(json_file, &PathBuf::from(&outbox_dir));
                        }
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
            info!("RUN_ONCE_AND_DIE: {} -- globus_xfer now ending", run_once_and_die);
            clean_up_and_exit(&pid_path, EXIT_SUCCESS);
        }

        // otherwise, sleep until we need to wake up again
        info!("Sleeping for {} seconds...", sleep_seconds);
        sleep(Duration::from_secs(sleep_seconds));
    }
}

fn process_work(
    context: &GlobusXferContext,
    work: &mut TaccSyncWork,
) -> Result<bool, Box<dyn Error>> {
    // log about what we're processing
    info!("Transferring files for {}: {} ({} files - {} bytes)", work.work_id, work.tape, work.files.len(), work.size);

    // how many of the files have finished?
    let mut finished_count = 0;

    // how many of the files have been updated?
    let mut update_count = 0;

    // for each file in the work unit
    for file in work.files.iter_mut() {
        // process the file
        let transfer_update = process_file(context, &work.work_id, file)?;
        // if the file was finished transferring
        if transfer_update.finished {
            finished_count += 1;
        }
        // if the file was updated (i.e.: a globus_task_id was added
        if transfer_update.updated {
            update_count += 1;
        }
    }

    // if any of the files were updated
    if update_count > 0 {
        // rewrite the work unit with the new globus_task_id values
        let inbox_dir = PathBuf::from(&context.inbox_dir);
        rewrite_work_unit(work, &inbox_dir)?;
    }

    // if all of the files were finished
    if finished_count >= work.files.len() {
        // we're all done; log about it and tell the caller we're done
        info!("All {} files have finished transferring. Will move to the outbox.", finished_count);
        return Ok(true);
    }

    // tell the caller we're not done yet
    info!("Not all files have finished transferring. Will check again next cycle.");
    Ok(false)
}

fn process_file(
    context: &GlobusXferContext,
    work_id: &Uuid,
    file: &mut TaccSyncFile,
) -> Result<TransferUpdate> {
    // log about what we're doing
    info!("Processing file: {}", file.file_name);

    // we'll keep score for the caller at home
    let mut transfer_update = TransferUpdate {
        finished: false,
        updated: false,
    };

    // if this file already has a Globus transfer
    if let Some(globus_task_id) = file.globus_task_id {
        // execute a command to check on the status of the transfer
        let globus = execute_globus_task_show(globus_task_id)?;

        // determine what we need to do, given the status of the transfer
        match globus.status.as_str() {
            // the transfer is in progress; maybe progressing, maybe not
            "ACTIVE" | "INACTIVE" => {
                info!("Task ID:{} has status {}.", globus_task_id, globus.status);
            },
            // the transfer has finished, we'll tell the caller that
            "SUCCEEDED" => {
                info!("Task ID:{} has SUCCEEDED.", globus_task_id);
                transfer_update.finished = true;
            },
            // the transfer has failed, this requires operator intervention
            "FAILED" | _ => {
                error!("Task ID:{} has status {}. Will send to quarantine.", globus_task_id, globus.status);
                return Err(anyhow::anyhow!("Quarantine due to failed Globus transfer."));
            },
        }
    } 
    // this file does not yet have a globus transfer
    else {
        // execute the command to create a globus transfer
        let globus = execute_globus_transfer(context, work_id, file)?;
        // take the task_id and update the file
        file.globus_task_id = Some(globus.task_id);
        // indicate that we updated the work unit
        transfer_update.updated = true;
    }

    // tell the caller about what happened
    Ok(transfer_update)
}

fn rewrite_work_unit(
    work: &mut TaccSyncWork, 
    inbox_dir: &PathBuf,
) -> Result<(), Box<dyn Error>> {
    // log about what we're doing
    let work_unit_path = inbox_dir.join(format!("{}.json", work.work_id));
    info!("Rewriting work unit: {}", work_unit_path.display());

    // rename the old work unit to a safety copy
    let safety_copy_build = format!("{}.safety", work_unit_path.display());
    let safety_copy_path = PathBuf::from(safety_copy_build);
    info!("Making a safety copy at: {}", safety_copy_path.display());
    fs::rename(&work_unit_path, &safety_copy_path)?;

    // rewrite the work unit for this tape group
    info!("Creating new work unit at: {}", work_unit_path.display());
    let file = File::create(work_unit_path)?;
    serde_json::to_writer_pretty(file, &work)?;

    // remove the safety copy after the successful rewrite
    info!("Removing the safety copy at: {}", safety_copy_path.display());
    // fs::remove_file(safety_copy_path)?;

    // tell the caller the work unit was successfully rewritten
    Ok(())
}

fn execute_globus_task_show(globus_task_id: Uuid) -> Result<GlobusTask> {
    // run the command: globus task show {globus_task_id}
    info!("Running command: globus task show {}", globus_task_id);
    let output = Command::new("globus")
        .arg("task")
        .arg("show")
        .arg("--format")
        .arg("json")
        .arg(globus_task_id.to_string())
        .output()?;

    // capture the output and deserialze the JSON
    let stdout = String::from_utf8(output.stdout)?;
    debug!("{}", stdout);

    // deserialize the GlobusTask and return it to the caller
    let globus: GlobusTask = serde_json::from_str(&stdout)?;

    // do some sanity checking here
    if globus.task_id != globus_task_id {
        error!("BAD MOJO -- We asked Globus about {} and we got information back on {} instead!", globus_task_id, globus.task_id);
        return Err(anyhow::anyhow!("{}", stdout));
    }

    // tell the caller what we got back from globus
    Ok(globus)
}

fn execute_globus_transfer(
    context: &GlobusXferContext,
    work_id: &Uuid,
    file: &mut TaccSyncFile,
) -> Result<TransferResult> {
    // do some sanity checking up front
    let hpss_base_path = &context.hpss_base_path;
    if !file.hpss_path.starts_with(hpss_base_path) {
        error!("BAD MOJO -- {} does not start with {}", file.hpss_path, context.hpss_base_path);
        return Err(anyhow::anyhow!("BAD MOJO -- {} does not start with {}", file.hpss_path, context.hpss_base_path));
    }

    // compute the fully qualified transfer paths
    let src_endpoint = &context.globus_source_endpoint;
    let transfer_dir = PathBuf::from(&context.transfer_dir);
    let src_xfer_dir = transfer_dir.join(work_id.to_string());
    let src_file = src_xfer_dir.join(&file.file_name);

    let dst_endpoint = &context.globus_dest_endpoint;
    let tacc_dir = PathBuf::from(&context.tacc_base_path);
    let start_index = hpss_base_path.len();
    let data_warehouse_path = &file.hpss_path[start_index + 1..];
    let dst_file = tacc_dir.join(data_warehouse_path);

    let src_path = format!("{}:{}", src_endpoint, src_file.display());
    let dst_path = format!("{}:{}", dst_endpoint, dst_file.display());

    // run the command: globus transfer {src} {dst}
    info!("Running command: globus transfer {} {}", src_path, dst_path);
    let output = Command::new("globus")
        .arg("transfer")
        .arg("--sync-level")
        .arg("mtime")
        .arg("--preserve-mtime")
        .arg("--verify-checksum")
        .arg("--format")
        .arg("json")
        .arg(src_path.to_string())
        .arg(dst_path.to_string())
        .output()?;

    // capture the output and deserialze the JSON
    let stdout = String::from_utf8(output.stdout)?;
    debug!("{}", stdout);

    // deserialize the GlobusTask and return it to the caller
    let globus: TransferResult = serde_json::from_str(&stdout)?;

    // and let's do a sanity check
    if globus.code != "Accepted" {
        error!("BAD MOJO -- Globus responded with {} instead of 'Accepted'", globus.code);
        return Err(anyhow::anyhow!("BAD MOJO -- Globus responded with {} instead of 'Accepted'", globus.code));
    }

    // tell the caller about the result of creating the transfer
    Ok(globus)
}
