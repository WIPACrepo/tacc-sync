# operator.md

## Motivation
We've got an archival storage allocation on the HPSS tape archive at NERSC.
We've also got an archival storage allocation on the Ranch tape archive at TACC.
We'd like to synchronize our data at NERSC to TACC.

## Operation
This section describes the operation of the `tacc-sync` software.

### Workspace
tacc-sync operates inside a workspace, usually defined by the
environment variable `TACC_SYNC_WORK_ROOT`. From here, we will refer to
the root of this workspace directory as `$TSWR`.

### Request Workflow
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

## Components
These are the components of `tacc-sync`.

### finisher
The finisher looks for requests in `$TSWR/request_queue` and then checks
for work that was generated for the request. If there are no work units
in the directories where work is processed, then the request is finished.

Finished requests are moved to `$TSWR/finished`. No further processing
of the request is expected after this.

### globus_xfer
The globus_xfer component schedules and monitors Globus transfers from
NERSC DTN nodes to TACC. It checks `$TSWR/globus_queue` for work, and
determines if a Globus transfer should be initiated, if a Globus
transfer is in progress, or if a Globus transfer has finished.

After the corresponding Globus transfer has finished, the work is moved
to the `$TSWR/reaper_queue` for removal from the transfer buffer on the
community file system.

### reaper
The reaper component removes files from the transfer buffer on the
community file system. It checks `$TSWR/reaper_queue` for work, and
removes all the files named in that work.

After the files have been removed from the transfer buffer on the
community file system, the work is moved to `$TSWR/finished`. No further
processing of the work is expected after this.

### retriever
The retriever component copies files from the HPSS file system at NERSC
to the transfer buffer on the community file system. It checks
`$TSWR/hpss_queue` for work, and then issues an hsi command to hpss to
copy each of the files to the transfer buffer.

After the files have been copied to the transfer buffer on the community
file system, the work is moved to `$TSWR/globus_queue` for transfer to
TACC.

### syncer
The syncer looks for requests in `$TSWR/inbox`. If a request is found
there, work will be generated for that request in `$TSWR/hpss_queue`.
The request is then moved to `$TSWR/request_queue` so that it can be
checked regularly for progress.
