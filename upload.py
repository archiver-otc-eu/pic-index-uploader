#!/usr/bin/env python3

import asyncio
import argparse
import shutil
import tempfile
import urllib.request
from urllib.parse import urlparse
import json
import httpx
import os
import logging
import ssl

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description="Register files in the Onedata system",
)

requiredNamed = parser.add_argument_group("required named arguments")

tstraw_prefix = "TSTRAW"

requiredNamed.add_argument(
    "-H", "--host", action="store", help="Oneprovider host", dest="host", required=True
)

requiredNamed.add_argument(
    "-spi",
    "--space-id",
    action="store",
    help="Id of the space in which the file will be registered.",
    dest="space_id",
    required=True,
)

requiredNamed.add_argument(
    "-sti",
    "--storage-id",
    action="store",
    help="Id of the storage on which the file is located. Storage must be created as an `imported` storage with path type equal to `canonical`.",
    dest="storage_id",
    required=True,
)

requiredNamed.add_argument(
    "-t",
    "--token",
    action="store",
    help="Onedata access token",
    dest="token",
    required=True,
)

requiredNamed.add_argument(
    "-i",
    "--index-url",
    action="store",
    help="Index file URL.",
    dest="index_url",
    required=True,
)

parser.add_argument(
    "-logging",
    "--logging-frequency",
    action="store",
    type=int,
    help="Frequency of logging. Log will occur after registering every logging_freq number of files",
    dest="logging_freq",
    default=None,
)


REGISTER_FILE_ENDPOINT = "https://{0}/api/v3/oneprovider/data/register"


async def register_file(storage_file_id, size, checksum):
    headers = {"X-Auth-Token": args.token, "content-type": "application/json"}
    payload = {
        "spaceId": args.space_id,
        "storageId": args.storage_id,
        "storageFileId": storage_file_id,
        "destinationPath": storage_file_id,
        "size": size,
        "mode": "664",
        "autoDetectAttributes": False,
        "xattrs": {"checksum": checksum, "checksum-algorithm": "adler-32"},
    }
    try:
        async with httpx.AsyncClient(verify=False, timeout=60) as client:
            await client.post(
                REGISTER_FILE_ENDPOINT.format(args.host), json=payload, headers=headers
            )
    except Exception as e:
        logging.error(
            "Registration of {0} failed due to {1}".format(storage_file_id, e),
            exc_info=True,
        )


def download_and_parse_tstraw_index(url):
    data = urllib.request.urlopen(url)
    res = []
    for file_data in data:
        file_name, file_size, file_adler_checksum = (
            file_data.decode("utf-8").rstrip("\n").split(" ")
        )
        res.append(
            (
                os.path.join("/", tstraw_prefix, file_name),
                int(file_size),
                file_adler_checksum,
            )
        )

    return res


if __name__ == "__main__":
    args = parser.parse_args()

    file_index = download_and_parse_tstraw_index(args.index_url)

    async_batch_size = 100
    i = 0
    chunk = file_index[i : i + async_batch_size]

    loop = asyncio.get_event_loop()

    while len(chunk) > 0:
        print(
            "Registering files {}:{} of {} at Oneprovider {}...".format(
                i, i + len(chunk), len(file_index), args.host
            )
        )

        tasks = [register_file(*f) for f in chunk]

        loop.run_until_complete(asyncio.wait(tasks))

        i += len(chunk)

        chunk = file_index[i : i + async_batch_size]

    loop.close()
