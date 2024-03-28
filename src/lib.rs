// lib.rs

use chrono::{DateTime, Utc};
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// HpssFile represents the file metadata returned by HPSS
#[derive(Clone)]
pub struct HpssFile {
    pub hpss_path: String,
    pub size: u64,
    pub tape: String,
    pub tape_num: u64,
    pub tape_offset: u64,
}

/// TaccSyncFile represents a file to synchronize from NERSC to TACC
#[derive(Debug, Deserialize, Serialize)]
pub struct TaccSyncFile {
    /// the name of the file without any path information
    pub file_name: String,
    /// absolute path of the file in HPSS
    pub hpss_path: String,
    /// recorded size of the file in HPSS
    pub size: u64,
    /// the tape mark
    pub tape_num: u64,
    /// how many bytes past the mark the file starts
    pub tape_offset: u64,
}


/// TaccSyncRequest represents a request to synchronize files from NERSC to TACC
#[derive(Debug, Deserialize, Serialize)]
pub struct TaccSyncRequest {
    /// this is the TaccSyncRequest id
    pub request_id: Uuid,
    /// the timestamp when this request was created
    pub date_created: DateTime<Utc>,
    pub source: String,
    pub dest: String,
    pub pattern: String,
}

/// TaccSyncWork represents a tape-grouped set of files to synchronize from NERSC to TACC
#[derive(Debug, Deserialize, Serialize)]
pub struct TaccSyncWork {
    /// this is the TaccSyncWork id
    pub work_id: Uuid,
    /// the timestamp when this work unit was created
    pub date_created: DateTime<Utc>,
    /// the HPSS tape(s) associated with this group of files
    pub tape: String,
    /// the total size of this group of files
    pub size: u64,
    /// this is the TaccSyncRequest id; the request that generated this work
    pub request_id: Uuid,
    /// the list of files to synchronize from NERSC to TACC
    pub files: Vec<TaccSyncFile>,
    /// the Globus transfer id of this work unit, if created
    pub transfer_id: Option<Uuid>,
}

/// Converts a string with truthy values into a boolean.
///
/// # Arguments
///
/// * `s` - A string slice that holds a truthy value
///
/// # Returns
///
/// True if the provided string slice contained a true-like truthy value:
/// TRUE, true, T, t, YES, yes, Y, y, 1
///
/// False otherwise.
pub fn boolify(s: &str) -> bool {
    let value = s.to_lowercase(); // normalize to lower case for comparison
    match value.as_str() {
        "true" | "t" | "yes" | "y" | "1" => true,
        "false" | "f" | "no" | "n" | "0" => false,
        _ => false, // no truth for you!
    }
}

/// Attempt to remove the provided pid file before exiting. If there is
/// an error removing pid file, log about it.
///
/// # Arguments
///
/// * `pid_path` - The pid file to be removed
/// * `exit_code` - The code to exit the application
pub fn clean_up_and_exit(pid_path: &str, exit_code: i32) {
    // attempt to remove the pid file
    info!("Removing pid file: {}", pid_path);
    if let Err(e) = std::fs::remove_file(pid_path) {
        error!("Failed to remove pid file: {}", e);
    }
    // exit the program with the provided exit code
    std::process::exit(exit_code);
}

/// Finds all .json files in the given directory path.
///
/// # Arguments
///
/// * `dir_path` - A string slice that holds the path to the directory to search.
///
/// # Returns
///
/// A vector of `PathBuf` representing paths to the .json files found. The vector will be empty on error.
pub fn find_json_files_in_directory(dir_path: &str) -> Vec<PathBuf> {
    let path = Path::new(dir_path);
    let mut json_files = Vec::new();

    // attempt to read the directory specified by `dir_path`
    match fs::read_dir(path) {
        Ok(entries) => {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                // check if the path is a file and ends with '.json'
                if path.is_file() && path.extension().and_then(std::ffi::OsStr::to_str) == Some("json") {
                    json_files.push(path);
                }
            }
        },
        Err(e) => {
            // log any errors encountered and return an empty vector
            error!("Error reading directory '{}': {}", dir_path, e);
        },
    }

    json_files
}

/// Load a TaccSyncRequest object from a JSON file.
///
/// # Arguments
///
/// * `file_path` - A PathBuf containing the path to the JSON file to be loaded
///
/// # Returns
///
/// Result containing a TaccSyncRequest object if loading was successful.
pub fn load_request_from_file(file_path: &PathBuf) -> Result<TaccSyncRequest> {
    let mut file = File::open(file_path).expect("file not found");
    let mut contents = String::new();
    file.read_to_string(&mut contents).expect("something went wrong reading the file");
    let r: TaccSyncRequest = serde_json::from_str(&contents)?;
    Ok(r)
}

/// Load a TaccSyncWork object from a JSON file.
///
/// # Arguments
///
/// * `file_path` - A PathBuf containing the path to the JSON file to be loaded
///
/// # Returns
///
/// Result containing a TaccSyncWork object if loading was successful.
pub fn load_work_from_file(file_path: &PathBuf) -> Result<TaccSyncWork> {
    let mut file = File::open(file_path).expect("file not found");
    let mut contents = String::new();
    file.read_to_string(&mut contents).expect("something went wrong reading the file");
    let r: TaccSyncWork = serde_json::from_str(&contents)?;
    Ok(r)
}

/// Moves the provided file to the provided destination directory.
///
/// # Arguments
///
/// * `file_path` - The path to the file to be moved.
/// * `dest_dir` - The path to the destination directory.
///
/// # Returns
///
/// A `Result` indicating success or failure.
pub fn move_to_outbox(file_path: &PathBuf, dest_dir: &PathBuf) {
    // if we can get the file name of the source file
    if let Some(file_name) = file_path.file_name() {
        // construct the destination path by appending the file name to the destination directory
        let dest_path = dest_dir.join(file_name);
        // attempt to move the file
        info!("Moving {} to {}", file_path.display(), dest_path.display());
        match fs::rename(&file_path, &dest_path) {
            Err(e) => {
                // if we can't move a file, better to stop immediately
                error!("Unable to rename: Unable to move {} to {}", file_path.display(), dest_dir.display());
                error!("Error: {}", e);
                panic!("FULL STOP -- Failed to perform basic but critical file system operation")
            },
            _ => return
        }
    }

    // if we can't move a file, better to stop immediately
    error!("Missing file_name: Unable to move {} to {}", file_path.display(), dest_dir.display());
    panic!("FULL STOP -- Failed to perform basic but critical file system operation")
}
