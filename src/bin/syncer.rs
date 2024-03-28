// syncer.rs

use chrono::Utc;
use globset::{Glob, GlobSetBuilder};
use log::{error, info};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;
use tacc_sync::{
    boolify, clean_up_and_exit, find_json_files_in_directory, load_request_from_file, move_to_outbox, HpssFile, TaccSyncFile, TaccSyncRequest, TaccSyncWork
};
use uuid::Uuid;

/// the process exit code indicating successful exit
const EXIT_SUCCESS: i32 = 0;

/// we expect 13 fields from an hsi metadata query
const NUM_HSI_METADATA_FIELDS: usize = 13;

/// the version of the package being compiled
const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
    // initialize logging
    env_logger::init();
    info!("tacc-sync v{} - syncer starting", VERSION);

    // load configuration from environment
    let hsi_base_path = std::env::var("HSI_BASE_PATH").expect("HSI_BASE_PATH environment variable not set");
    let inbox_dir = std::env::var("INBOX_DIR").expect("INBOX_DIR environment variable not set");
    let outbox_dir = std::env::var("OUTBOX_DIR").expect("OUTBOX_DIR environment variable not set");
    let pid_path = std::env::var("PID_PATH").expect("PID_PATH environment variable not set");
    let quarantine_dir = std::env::var("QUARANTINE_DIR").expect("QUARANTINE_DIR environment variable not set");
    let run_once_and_die = std::env::var("RUN_ONCE_AND_DIE").expect("RUN_ONCE_AND_DIE environment variable not set");
    let semaphore_dir = std::env::var("SEMAPHORE_DIR").expect("SEMAPHORE_DIR environment variable not set");
    let work_dir = std::env::var("WORK_DIR").expect("WORK_DIR environment variable not set");
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
                process_sync_request(&request, &hsi_base_path, &PathBuf::from(&semaphore_dir), &PathBuf::from(&work_dir));
                // move the request to the outbox
                move_to_outbox(json_file, &PathBuf::from(&outbox_dir));
            }
            // we weren't able to load the sync request
            else {
                error!("Unable to load TaccSyncRequest: {}", json_file_str);
                move_to_outbox(json_file, &PathBuf::from(&quarantine_dir));
            }
        }

        // if this was a one-shot adventure
        if run_once {
            info!("RUN_ONCE_AND_DIE: {} -- syncer now ending", run_once_and_die);
            clean_up_and_exit(&pid_path, EXIT_SUCCESS);
        }

        // otherwise, sleep until we need to wake up again
        info!("Sleeping for {} seconds...", sleep_seconds);
        sleep(Duration::from_secs(sleep_seconds));
    }
}

fn process_sync_request(request: &TaccSyncRequest, hsi_base_path: &str, semaphore_dir: &PathBuf, work_dir: &PathBuf) {
    // query hsi for all icecube files
    let paths = query_hsi_all_files(hsi_base_path);
    // filter out the icecube files that match the pattern
    let request_files = filter_request_files(paths, &request.pattern);
    // query hsi for file metadata including tape location
    let file_metadata = query_hsi_tape_metadata(request_files, semaphore_dir);
    // sort hsi metadata by tape and position
    let mut hpss_files = parse_tape_metadata(file_metadata);
    // group the metadata into per-tape groups
    let tape_groups = group_files_by_tape(&mut hpss_files);
    // generate per-tape work units
    generate_work_units(request, &tape_groups, work_dir);
}

fn query_hsi_all_files(hsi_base_path: &str) -> Vec<String> {
    // run the hsi command to get a list of all our files
    info!("Querying hsi for files: {}", hsi_base_path);
    let output = Command::new("hsi")
        .arg("-q")
        .arg("ls")
        .arg("-1")
        .arg("-R")
        .arg(hsi_base_path)
        .output()
        .expect("Unable to query hsi for file metadata: hsi -q ls -1 -R $HSI_BASE_PATH");

    // convert the output to a String (NOTE: stderr not stdout!)
    let stderr = String::from_utf8(output.stderr).expect("hsi output does not conform to utf8 encoding");

    // return the vector containing one directory or file per line
    stderr.lines().map(ToString::to_string).collect::<Vec<String>>()
}

fn filter_request_files(paths: Vec<String>, pattern: &str) -> Vec<String> {
    // build the filter predicate
    info!("Filtering paths by request pattern: {}", pattern);
    let glob = Glob::new(pattern).expect("invalid glob pattern");
    let glob_set = GlobSetBuilder::new().add(glob).build().expect("failed to build glob set");

    // filter the paths based on the glob set
    paths.into_iter()
        .filter(|path| {
            glob_set.is_match(path)
        })
        .collect::<Vec<String>>()
}

fn query_hsi_tape_metadata(request_files: Vec<String>, semaphore_dir: &PathBuf) -> Vec<String> {
    // log about what we're doing
    info!("Querying hsi for tape metadata for {} files", request_files.len());

    // create a temporary file we can feed to hsi
    let file_name = Uuid::new_v4().to_string();
    let hsi_batch_file = semaphore_dir.join(file_name);
    info!("hsi batch file: {}", hsi_batch_file.display());

    // we batch the hsi tape metadata commands into the file
    let file = File::create(&hsi_batch_file).expect("Unable to create hsi batch temporary file");
    let mut writer = BufWriter::new(file);
    for path in request_files {
        writeln!(writer, "ls -NP {}", path).expect("Unable to write to hsi batch temporary file");
    }
    writer.flush().expect("Unable to close hsi batch temporary file");

    // run the hsi command, feeding it the batch file
    let output = Command::new("hsi")
        .arg("-P")
        .arg("in")
        .arg(&hsi_batch_file)
        .output()
        .expect("Unable to query hsi for file metadata: hsi -P in $HSI_BATCH_FILE");

    // remove our temporary file
    info!("Removing hsi batch file: {}", hsi_batch_file.display());
    std::fs::remove_file(hsi_batch_file).expect("Unable to delete hsi batch temporary file");

    // convert the output to a String (NOTE: stdout not stderr!)
    let stdout = String::from_utf8(output.stdout).expect("hsi output does not conform to utf8 encoding");

    // return the vector containing metadata for one file per line
    stdout.lines().map(ToString::to_string).collect::<Vec<String>>()
}

fn parse_tape_metadata(file_metadata: Vec<String>) -> Vec<HpssFile> {
    // parse the metadata lines
    info!("Parsing metadata from {} hsi files into HpssFile objects", file_metadata.len());
    let mut hpss_files = Vec::new();

    // hpss output will come back like this:
    // ls -NP /home/projects/icecube/data/exp/IceCube/2009/unbiased/PFRaw/0101/cd88bb827ab811eba0ccfac645b4ea48.zip
    // FILE    /home/projects/icecube/data/exp/IceCube/2009/unbiased/PFRaw/0101/cd88bb827ab811eba0ccfac645b4ea48.zip   99658060045     99658060045     840+0   AG084600        5       0       1       03/01/2021      11:15:47        03/01/2021
    //         11:30:52

    // we care about the second line (the response to the command) ...
    //  0 // FILE
    //  1 // /home/projects/icecube/data/exp/IceCube/2011/unbiased/PFRaw/1109/b26eac34-7848-49de-a7c2-193e954af803.zip
    //  2 // 568860644320
    //  3 // 568860644320
    //  4 // 119+558936243566
    //  5 // AU031800,AU031900
    //  6 // 12
    //  7 // 0
    //  8 // 1
    //  9 // 04/07/2017
    // 10 // 02:19:14
    // 11 // 04/07/2017
    // 12 // 03:07:47
    // 13 ........................ length
    for metadata in file_metadata {
        let fields = metadata.split('\t').map(|s| s.to_string()).collect::<Vec<String>>();

        // if fields[0] is not 'FILE', it's probably the command; ignore it
        if fields[0] != "FILE" {
            continue;
        }

        // if we didn't get the proper number of fields, it is BAD MOJO
        if fields.len() != NUM_HSI_METADATA_FIELDS {
            // log about it and die; we leave no file behind!
            error!("hsi metadata parse error: NUM_HSI_METADATA_FIELDS={}, fields.len()={}", NUM_HSI_METADATA_FIELDS, fields.len());
            error!("Line: {}", metadata);
            panic!("BAD MOJO - hsi metadata parse error: NUM_HSI_METADATA_FIELDS");
        }

        // if the tape is specified, use it, otherwise call it "0"
        let tape = if fields[5].len() < 3 { "0" } else { &fields[5] };

        // if fields[4] has a + we've got tape number and offset, otherwise call them "0"
        let mut tape_num = String::from("0");
        let mut tape_offset = String::from("0");
        if fields[4].contains('+') {
            let tape_pos = fields[4].split("+").map(|s| s.to_string()).collect::<Vec<String>>();
            tape_num = tape_pos[0].clone();
            tape_offset = tape_pos[1].clone();
        }

        // add this file to the list of files we need to copy
        hpss_files.push(HpssFile {
            hpss_path: fields[1].clone(),
            size: fields[2].parse().unwrap(),
            tape: String::from(tape),
            tape_num: tape_num.parse().unwrap(),
            tape_offset: tape_offset.parse().unwrap(),
        });
    }

    // return the list of files we need to copy to the caller
    hpss_files
}

fn group_files_by_tape(hpss_files: &mut Vec<HpssFile>) -> Vec<Vec<HpssFile>> {
    info!("Grouping {} HpssFile objects into tape groups", hpss_files.len());

    // sort the vector by tape, tape_num, tape_offset, hpss_path
    hpss_files.sort_by(|a, b| {
        a.tape.cmp(&b.tape)
            .then_with(|| a.tape_num.cmp(&b.tape_num))
            .then_with(|| a.tape_offset.cmp(&b.tape_offset))
            .then_with(|| a.hpss_path.cmp(&b.hpss_path))
    });

    // group files into work units according to tape
    let mut grouped: Vec<Vec<HpssFile>> = Vec::new();
    let mut current_group: Vec<HpssFile> = Vec::new();

    for hpss_file in hpss_files {
        if current_group.is_empty() || current_group[0].tape == hpss_file.tape {
            current_group.push(hpss_file.clone());
        } else {
            grouped.push(current_group);
            current_group = vec![hpss_file.clone()];
        }
    }

    if !current_group.is_empty() {
        grouped.push(current_group);
    }

    // return the tape-grouped files
    grouped
}

fn generate_work_units(request: &TaccSyncRequest, tape_groups: &Vec<Vec<HpssFile>>, work_dir: &PathBuf) {
    // generate work units in the work directory
    info!("Generating {} work units in work directory: {}", tape_groups.len(), work_dir.display());
    for (index, tape_group) in tape_groups.iter().enumerate() {
        // log about what we're processing
        let mut size = 0;
        for file in tape_group {
            size += file.size;
        }
        info!("Processing {}/{}: {} ({} files - {} bytes)", index+1, tape_groups.len(), tape_group[0].tape, tape_group.len(), size);

        // for each HpssFile in this tape group
        let mut tacc_sync_files = Vec::new();
        for hpss_file in tape_group {
            // create a TaccSyncFile for that HpssFile
            let path = Path::new(&hpss_file.hpss_path);
            let file_name = path.file_name().expect("Unable to get file_name from hpss_path");
            tacc_sync_files.push(TaccSyncFile {
                file_name: file_name.to_string_lossy().to_string(),
                hpss_path: hpss_file.hpss_path.clone(),
                size: hpss_file.size,
                tape_num: hpss_file.tape_num,
                tape_offset: hpss_file.tape_offset,
            });
        }

        // create a TaccSyncWork work unit for this tape group
        let tacc_sync_work = TaccSyncWork {
            date_created: Utc::now(),
            files: tacc_sync_files,
            request_id: request.request_id,
            size,
            tape: tape_group[0].tape.clone(),
            work_id: Uuid::new_v4(),
            transfer_id: None,
        };

        // write the work unit for this tape group
        let work_unit_path = work_dir.join(format!("{}.json", tacc_sync_work.work_id));
        info!("Writing work unit to {}", work_unit_path.display());
        let file = File::create(work_unit_path).expect("Unable to create file for work unit");
        serde_json::to_writer_pretty(file, &tacc_sync_work).expect("Unable to write JSON to work unit file");
    }
}
