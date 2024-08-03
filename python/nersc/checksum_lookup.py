# checksum_lookup.py
"""Implement the Checksum-Lookup component; ask the File Catalog for sha512 checksums."""

import asyncio
import json
import logging
import os
import shutil
from subprocess import PIPE, run
from typing import Any, cast, Dict, List

from rest_tools.client import ClientCredentialsAuth, RestClient
from wipac_dev_tools.enviro_tools import from_environment, KeySpec

Context = Dict[str, Any]
JsonObj = Dict[str, Any]
WorkUnit = Dict[str, Any]


EXPECTED_CONFIG: KeySpec = {
    "AUTH_OPENID_URL": "https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
    "CLIENT_ID": "long-term-archive",
    "CLIENT_SECRET": None,  # must be provided
    "FILE_CATALOG_REST_URL": "https://file-catalog.icecube.wisc.edu",
    "INBOX_DIR": "/global/homes/i/icecubed/tacc-sync/work/checksum_queue",
    "JADE_LTA_DB_PATH": "jade-lta-db.json",
    "LOG_LEVEL": "NOTSET",
    "LOG_PATH": "/global/homes/i/icecubed/tacc-sync/work/log/checksum_lookup.log",
    "OUTBOX_DIR": "/global/homes/i/icecubed/tacc-sync/work/hpss_queue",
    "PID_PATH": "/global/homes/i/icecubed/tacc-sync/work/semaphore/checksum_lookup.pid",
    "QUARANTINE_DIR": "/global/homes/i/icecubed/tacc-sync/work/quarantine/checksum_lookup",
    "RUN_ONCE_AND_DIE": "TRUE",
    "SEMAPHORE_DIR": "/global/homes/i/icecubed/tacc-sync/work/semaphore",
    "WORK_SLEEP_SECONDS": "3600",
}

LOG = logging.getLogger(__name__)

TRUE_SET = {'1', 't', 'true', 'y', 'yes'}


def boolify(value: str) -> bool:
    """Convert a string into a True or False value."""
    return isinstance(value, str) and value.lower() in TRUE_SET


async def do_work(context: Context) -> None:
    """Process work units to add File Catalog checksums."""
    inbox_dir = context["INBOX_DIR"]
    quarantine_dir = context["QUARANTINE_DIR"]

    # get the work units in that inbox
    files = list_inbox(inbox_dir)
    # for each file in the inbox
    count = 0
    for file in files:
        count += 1
        LOG.info(f"Processing {count}/{len(files)}: {file}")
        # load the JSON into a work unit object
        work_unit = load_json(file)
        # try to process the work unit
        try:
            await lookup_work_unit(context, file, work_unit)
        except Exception as e:
            LOG.error(f"Error while handling work unit {file}")
            LOG.error(f"Error was {e}")
            move_to_outbox(file, quarantine_dir)


def list_inbox(inbox_dir: str) -> List[str]:
    """Get the contents of the inbox with full paths."""
    inbox_files = []

    files = os.listdir(inbox_dir)
    for file in files:
        inbox_file_path = os.path.join(inbox_dir, file)
        inbox_files.append(inbox_file_path)

    return inbox_files


def load_json(json_path: str) -> JsonObj:
    """Load JSON from the specified file as a Dict."""
    LOG.info(f"Loading JSON from {json_path}")
    with open(json_path, "r") as f:
        json_obj = json.load(f)
        # this cast makes some assumptions about our data
        # for example, technically [] is legal JSON
        # it may not end well if you try to load something like that
        return cast(JsonObj, json_obj)


async def lookup_checksum(context: Context, file: JsonObj) -> str:
    # first, try to query for the checksum from the File Catalog
    try:
        return await query_file_catalog(context, file)
    except Exception as e:
        LOG.error(f"Message: {e}")

    # if that failed, try to look up the checksum in the old JADE LTA database
    # here, we use a JSON export of the JADE LTA database as a proxy for the
    # database itself, because who wants to run MySQL at NERSC, amirite?
    file_name = file["file_name"]
    jade_lta_db = context["JADE_LTA_DB"]

    # Rows exported from the JADE LTA database look like this:
    # {
    #     "jade_bundle_id": 89,
    #     "bundle_file": "1f255bed-31d9-430e-88fa-379748339d81.zip",
    #     "checksum": "a5b2908a71765eeecf3c5770bf71dd0a068502ccbaffc280b8c113a7f3d435d15078e0bec77d3798bf16af6d19b22c57b5c5ecc65f131a29131d8368fa5644d9",
    #     "closed": 1,
    #     "date_created": "2016-09-07T12:15:11",
    #     "date_updated": "2016-09-07T12:39:27",
    #     "destination": "/data/exp/IceCube/2008/filtered/PFFilt/1122",
    #     "size": 32995815864,
    #     "uuid": "1f255bed-31d9-430e-88fa-379748339d81",
    #     "version": 307,
    #     "extension": 0
    # },

    # for each bundle
    for bundle in jade_lta_db["bundles"]:
        # if the bundle filename is plum
        if bundle["bundle_file"] == file_name:
            # then this is our checksum!
            return {
                "sha512": bundle["checksum"],
            }

    # whoops; no love -- no checksum in catalog, no checksum in JADE LTA DB
    raise Exception("Checksum not found in File Catalog or JADE LTA DB!")


async def lookup_work_unit(context: Context, work_unit_path: str, work_unit: WorkUnit) -> None:
    """Look up the checksums for the files in a work unit."""
    outbox_dir = context["OUTBOX_DIR"]

    # our work unit looks like this
    # {
    #     "work_id": "00a103a2-76e2-466b-ab32-ceeb591dd0d6",
    #     "date_created": "2024-03-28T00:15:14.988742277Z",
    #     "tape": "AG787200",
    #     "size": 748246351402,
    #     "request_id": "f8c7f5f6-d70a-47f5-bd0c-a8a2b896222f",
    #     "files": [
    #         {
    #             "file_name": "38748f7e927011eb8013bedaff42a7c6.zip",
    #             "hpss_path": "/home/projects/icecube/data/exp/IceCube/2019/unbiased/PFRaw/1222/38748f7e927011eb8013bedaff42a7c6.zip",
    #             "size": 106877814497,
    #             "tape_num": 31,
    #             "tape_offset": 0
    #         },
    #         ...
    #     ]
    # }

    # for each file in the work unit
    count = 0
    files = work_unit["files"]
    for file in files:
        count += 1
        file_name = file['file_name']
        LOG.info(f"Processing {count}/{len(files)}: {file_name}")
        # if we've already got a checksum, skip this file
        if "checksum" in file and file["checksum"] is not None:
            LOG.warn(f"File {file_name} already has checksum: {file['checksum']}")
            continue
        # since we don't have a checksum, we need to look it up
        checksum = lookup_checksum(context, file)
        file['checksum'] = checksum
        LOG.info(f"Found checksum for File {file_name}: {file['checksum']}")

    # convert the work_unit object to pretty-printed JSON
    json_work_unit = json.dumps(work_unit, indent=4)

    # write the JSON back to the file
    LOG.info(f"Just in case something goes wrong:\n{work_unit}")
    LOG.info(f"Writing the work unit back to {work_unit_path}")
    with open(work_unit_path, "w", encoding="utf-8") as outfile:
        outfile.write(json_work_unit)
        outfile.write("\n")

    # send the work unit downstream for further processing
    move_to_outbox(work_unit_path, outbox_dir)


def move_to_outbox(inbox_file: str, outbox_dir: str) -> None:
    """Move the provided file to the provided output directory."""
    LOG.info(f"Moving file {inbox_file} to output directory {outbox_dir}")
    outbox_file = os.path.join(outbox_dir, os.path.basename(inbox_file))
    LOG.info(f"shutil.move {inbox_file} -> {outbox_file}")
    shutil.move(inbox_file, outbox_file)


async def query_file_catalog(context: Context, file_obj: JsonObj) -> str:
    """Query the FileCatalog for the bundle entry, and return the sha512sum."""
    fc_rc = context["FILE_CATALOG_CLIENT"]

    # our work unit looks like this
    # {
    #     "work_id": "00a103a2-76e2-466b-ab32-ceeb591dd0d6",
    #     "date_created": "2024-03-28T00:15:14.988742277Z",
    #     "tape": "AG787200",
    #     "size": 748246351402,
    #     "request_id": "f8c7f5f6-d70a-47f5-bd0c-a8a2b896222f",
    #     "files": [
    #         {
    #             "file_name": "38748f7e927011eb8013bedaff42a7c6.zip",
    #             "hpss_path": "/home/projects/icecube/data/exp/IceCube/2019/unbiased/PFRaw/1222/38748f7e927011eb8013bedaff42a7c6.zip",
    #             "size": 106877814497,
    #             "tape_num": 31,
    #             "tape_offset": 0
    #         },
    #         ...
    #     ]
    # }

    # pull fields out of the file_obj
    file_name = file_obj["file_name"]
    hpss_path = file_obj["hpss_path"]
    # determine which file catalog record to pull up
    bundle_uuid, _ = os.path.splitext(file_name)
    # query the File Catalog for the bundle file record
    try:
        LOG.info(f"GET /api/files/{bundle_uuid} - {hpss_path}")
        bundle_record = await fc_rc.request("GET", f"/api/files/{bundle_uuid}")
    except Exception as e:
        LOG.error(f"Error: GET /api/files/{bundle_uuid} - {hpss_path}")
        LOG.error(f"Message: {e}")
        raise Exception(f"Unable to query File Catalog for file: {hpss_path}")
    # ensure these are the droids that we're looking for
    if bundle_record["logical_name"] != hpss_path:
        LOG.error(f"File Catalog Query for {bundle_uuid} to load record for {hpss_path}")
        LOG.error(f"Instead got File Catalog record for {bundle_record['logical_name']}")
        raise Exception(f"File Catalog record mismatch. Expected:{hpss_path} Found:{bundle_record['logical_name']}")
    # return the sha512 checksum for the bundle to the caller
    LOG.info(f"{bundle_record['checksum']}  {hpss_path}")
    return bundle_record["checksum"]


# -----------------------------------------------------------------------------


async def main(context: Context) -> None:
    """Perform asynchronous setup tasks and start the application."""
    pid_path = context["PID_PATH"]
    run_once_and_die = context["RUN_ONCE_AND_DIE"]
    work_sleep_seconds = context["WORK_SLEEP_SECONDS"]

    LOG.info("Starting asynchronous code")

    # as long as our pid file still exists, perform a work cycle
    while os.path.exists(pid_path):
        LOG.info("Begin work cycle")

        await do_work(context)

        # if this was a one-shot adventure
        if run_once_and_die:
            LOG.info(f"RUN_ONCE_AND_DIE: {run_once_and_die} -- checksum_lookup now ending")
            LOG.info(f"Removing pid file: {pid_path}")
            os.remove(pid_path)
            break

        LOG.info(f"Sleeping for {work_sleep_seconds} seconds until next work cycle")
        await asyncio.sleep(work_sleep_seconds)

    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Perform synchronous setup tasks and start the application."""
    config = from_environment(EXPECTED_CONFIG)

    log_level = getattr(logging, cast(str, config["LOG_LEVEL"]).upper())
    log_path = cast(str, config["LOG_PATH"])
    logging.basicConfig(
        filename=log_path,
        style="{",
        format="{asctime} [{threadName}] {levelname:5} ({filename}:{lineno}) - {message}",
        level=log_level,
    )
    LOG.info("Starting synchronous code")

    # configure a RestClient to talk to the File Catalog
    client = ClientCredentialsAuth(
        address = config["FILE_CATALOG_REST_URL"],
        token_url = config["AUTH_OPENID_URL"],
        client_id = config["CLIENT_ID"],
        client_secret = config["CLIENT_SECRET"],
    )

    # create a Context object for the application
    context: Context = {
        "FILE_CATALOG_CLIENT": client,
        "INBOX_DIR": cast(str, config["INBOX_DIR"]),
        "JADE_LTA_DB": load_json(config["JADE_LTA_DB_PATH"]),
        "OUTBOX_DIR": cast(str, config["OUTBOX_DIR"]),
        "PID_PATH": cast(str, config["PID_PATH"]),
        "QUARANTINE_DIR": cast(str, config["QUARANTINE_DIR"]),
        "RUN_ONCE_AND_DIE": cast(bool, boolify(config["RUN_ONCE_AND_DIE"])),
        "SEMAPHORE_DIR": cast(str, config["SEMAPHORE_DIR"]),
        "WORK_SLEEP_SECONDS": int(config["WORK_SLEEP_SECONDS"]),
    }

    asyncio.run(main(context))

    LOG.info("Ending synchronous code")


if __name__ == '__main__':
    main_sync()
