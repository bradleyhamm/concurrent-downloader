#!/usr/bin/python3
import os
import sys
import tempfile
import typing
import urllib.request
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus

DEFAULT_CHUNK_SIZE = 2 ** 20
DEFAULT_MAX_WORKERS = 20
TIMEOUT_SECONDS = 20


def download_chunk(url: str, range_start: int, range_end: int, outfile: str):
    headers = {"Range": "bytes={}-{}".format(range_start, range_end)}
    req = urllib.request.Request(url, method="GET", headers=headers)
    with urllib.request.urlopen(req) as res:
        if res.status >= 400:
            raise Exception("received status code {}: {} - {}".format(res.status, res.reason, res.message))
        if res.status != HTTPStatus.PARTIAL_CONTENT:
            raise Exception("partial content not supported")
        with open(outfile, "wb") as out:
            out.write(res.read())
            out.flush()
            os.fsync(out.fileno())
            out.close()
    return range_start, range_end


def download(url: str, out: typing.IO, chunk_size: int, max_workers: int):
    with urllib.request.urlopen(url) as head_res:
        if not head_res.headers["content-length"]:
            raise Exception("cannot determine total download size")
        size = int(head_res.headers["content-length"])
        futures = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            start = 0
            while start < size:
                end = min(start + chunk_size - 1, size)
                chunk_file = tempfile.mktemp()
                future = executor.submit(download_chunk, url, start, end, chunk_file)
                futures.append((future, chunk_file))
                start = end + 1
        for future, chunk_file in futures:
            try:
                future.result(timeout=TIMEOUT_SECONDS)
            except Exception:
                for f, _ in futures:
                    if f is not future:
                        f.cancel()
                raise
            with open(chunk_file, "rb") as chunk:
                out.write(chunk.read())


def main():
    parser = ArgumentParser(description="Download a file \"quickly\"")
    parser.add_argument("--chunk-size", "-c", type=int, help="Chunk size (in bytes)", default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--max-workers", "-w", type=int, help="Number of workers", default=DEFAULT_MAX_WORKERS)
    parser.add_argument("--outfile", "-o", type=str, help="Outfile (omit for stdout)")
    parser.add_argument("url", type=str, help="URL to download")

    args = parser.parse_args()

    if args.outfile:
        with open(args.outfile, "wb") as out:
            download(args.url, out, args.chunk_size, args.max_workers)
            out.flush()
            os.fsync(out.fileno())
    else:
        download(args.url, sys.stdout, args.chunk_size, args.max_workers)


if __name__ == '__main__':
    main()
