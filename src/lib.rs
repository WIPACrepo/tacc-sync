// lib.rs

use chrono::{DateTime, Utc};
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::fs::{self, File};
use std::io::{self, BufRead, Read};
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Clone)]
pub struct HpssFile {
    pub hpss_path: String,
    pub size: u64,
    pub tape: String,
    pub tape_num: u64,
    pub tape_offset: u64,
}

/// TaccSyncRequest represents a request to synchronize files from NERSC to TACC
#[derive(Debug, Deserialize, Serialize)]
pub struct TaccSyncRequest {
    pub date_created: DateTime<Utc>,
    pub dest: String,
    pub pattern: String,
    pub request_id: Uuid,
    pub source: String,
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

/// TODO
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
pub fn load_request_from_file(file_path: &PathBuf) -> Result<TaccSyncRequest> {
    let mut file = File::open(file_path).expect("file not found");
    let mut contents = String::new();
    file.read_to_string(&mut contents).expect("something went wrong reading the file");
    let r: TaccSyncRequest = serde_json::from_str(&contents)?;
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
        info!("Moving {} to {}", file_path.to_string_lossy(), dest_path.to_string_lossy());
        match fs::rename(&file_path, &dest_path) {
            Err(e) => {
                // if we can't move a file, better to stop immediately
                error!("Unable to rename: Unable to move {} to {}", file_path.to_string_lossy(), dest_dir.to_string_lossy());
                error!("Error: {}", e);
                panic!("FULL STOP -- Failed to perform basic but critical file system operation")
            },
            _ => return
        }
    }

    // if we can't move a file, better to stop immediately
    error!("Missing file_name: Unable to move {} to {}", file_path.to_string_lossy(), dest_dir.to_string_lossy());
    panic!("FULL STOP -- Failed to perform basic but critical file system operation")
}

// --------------------------------------------------------------------------

pub fn read_paths_from_file<P: AsRef<Path>>(file_path: P) -> io::Result<Vec<String>> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    let mut paths = Vec::new();

    for line in reader.lines() {
        if let Ok(path) = line {
            paths.push(path);
        }
    }

    Ok(paths)
}
