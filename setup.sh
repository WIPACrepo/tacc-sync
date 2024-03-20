#!/usr/bin/env bash
# setup.sh

export TACC_SYNC_WORK_ROOT=${TACC_SYNC_WORK_ROOT:="/global/homes/i/icecubed/tacc-sync/workspace"}

# if Rust is not installed
if ! command -v rustc &> /dev/null
then
    # install Rust
    echo "Rust is not installed. Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    # add Rust to the PATH
    source ${HOME}/.cargo/env
fi

# create tacc-sync workspace under TACC_SYNC_WORK_ROOT
mkdir -p "${TACC_SYNC_WORK_ROOT}/finished"
mkdir -p "${TACC_SYNC_WORK_ROOT}/globus_queue"
mkdir -p "${TACC_SYNC_WORK_ROOT}/hpss_queue"
mkdir -p "${TACC_SYNC_WORK_ROOT}/inbox"
mkdir -p "${TACC_SYNC_WORK_ROOT}/log"
mkdir -p "${TACC_SYNC_WORK_ROOT}/quarantine"
mkdir -p "${TACC_SYNC_WORK_ROOT}/quarantine/finisher"
mkdir -p "${TACC_SYNC_WORK_ROOT}/quarantine/globus_xfer"
mkdir -p "${TACC_SYNC_WORK_ROOT}/quarantine/reaper"
mkdir -p "${TACC_SYNC_WORK_ROOT}/quarantine/retriever"
mkdir -p "${TACC_SYNC_WORK_ROOT}/quarantine/syncer"
mkdir -p "${TACC_SYNC_WORK_ROOT}/reaper_queue"
mkdir -p "${TACC_SYNC_WORK_ROOT}/request_queue"
mkdir -p "${TACC_SYNC_WORK_ROOT}/semaphore"
