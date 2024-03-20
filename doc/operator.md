# operator.md

## Motivation
We've got an archival storage allocation on the HPSS tape archive at NERSC.
We've also got an archival storage allocation on the Ranch tape archive at TACC.
We'd like to synchronize our data at NERSC to TACC.

## Operation
The software will sy

### Workspace
tacc-sync operates inside a workspace, usually defined by the
environment variable `TACC_SYNC_ROOT`. From here, we will refer to the
root of this workspace directory as `$TSR`

### Request Workflow
123456789012345678901234567890123456789012345678901234567890123456789012
A request is a command to synchronize a particular dataset from NERSC to
TACC.

    $TSR/inbox
    $TSR/request_queue
    $TSR/finished


### Sync Workflow
A sync is a command to synchronize a particular subset of data files
from NERSC to TACC. The data files are selected according to the tape
on which they are stored. This ensures the HPSS tape robot is not
swamped with inefficent requests to load/unload tapes.

    $TSR/hpss_queue
    $TSR/globus_queue
    $TSR/reaper_queue
