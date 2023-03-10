#!/usr/bin/env python3

"""
Used for automatically uploading files to S3
"""

import argparse
import logging as log
import boto3
import os
import time
import hashlib


def get_args():
    """
    get_args controls the argparse usage for the script. It sets up and parses
    arguments and returns them in dict format
    """
    parser = argparse.ArgumentParser(description="Options for the upload script")
    parser.add_argument("--verbose", action="store_true",
                        help="Print debug logs", default=False)
    parser.add_argument("--log", type=str, required=False,
                        help="Path to output log information",
                        default="/tmp/upload.log")
    parser.add_argument("--s3-bucket", type=str, required=False,
                        help="S3 binary bucket name",
                        default="elixxir-bins")
    parser.add_argument("--s3-region", type=str, required=False,
                        help="S3 region",
                        default="us-west-1")
    parser.add_argument("--s3-access-key", type=str, required=True,
                        help="S3 access key")
    parser.add_argument("--s3-secret", type=str, required=True,
                        help="S3 access key secret")
    parser.add_argument("--local-path", type=str, required=True,
                        help="Path to file to upload")
    parser.add_argument("--remote-path", type=str, required=True,
                        help="Remote location to place the file")
    parser.add_argument("--upload-frequency", type=int, required=False,
                        help="Frequency of file uploads (in seconds)",
                        default=60)
    parser.add_argument("--truncate-size", type=int, required=False,
                        help="Maximum size of file before it is truncated (in MB)",
                        default=0)

    args = vars(parser.parse_args())
    log.basicConfig(format='[%(levelname)s] %(asctime)s: %(message)s',
                    level=log.DEBUG if args['verbose'] else log.INFO,
                    datefmt='%d-%b-%y %H:%M:%S',
                    filename=args["log"])
    return args


def upload(src_path, dst_path, s3_bucket, region,
           access_key_id, access_key_secret):
    """
    Uploads file at src_path to dst_path on s3_bucket using
    the provided access_key_id and access_key_secret.

    :param src_path: Path of the local file
    :type src_path: str
    :param dst_path: Path of the destination on S3 bucket
    :type dst_path: str
    :param s3_bucket: Name of S3 bucket
    :type s3_bucket: str
    :param region: Region of S3 bucket
    :type region: str
    :param access_key_id: Access key ID for bucket access
    :type access_key_id: str
    :param access_key_secret: Access key secret for bucket access
    :type access_key_secret: str
    :return: None
    :rtype: None
    """
    try:
        upload_data = open(src_path, 'rb')
        s3 = boto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=access_key_secret,
            region_name=region).resource("s3")
        s3.Bucket(s3_bucket).put_object(Key=dst_path, Body=upload_data.read())
        log.info("Successfully uploaded to {}/{} from {}".format(s3_bucket,
                                                                 dst_path,
                                                                 src_path))
    except Exception as e:
        log.error("Unable to upload {} to S3: {}".format(src_path, e))


def get_hash(file_path):
    """
    Obtain the hash of the file at the given path

    :param file_path: Path of the file to hash
    :return: Hash of the file
    """
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        buf = f.read()
    hasher.update(buf)
    return hasher.hexdigest()


def main():
    args = get_args()
    log.info("Running with configuration: {}".format(args))

    # Size of one megabyte in bytes
    MEGABYTE = 1048576

    upload_frequency = args["upload_frequency"]
    truncate_size = args["truncate_size"] * MEGABYTE
    s3_bucket_name = args["s3_bucket"]
    s3_access_key_id = args["s3_access_key"]
    s3_access_key_secret = args["s3_secret"]
    s3_bucket_region = args["s3_region"]
    remote_path = args['remote_path']
    local_path = os.path.expanduser(args['local_path'])

    # Wait for the file to exist
    log.info(f"Waiting for {local_path}...")
    while not os.path.exists(local_path):
        time.sleep(upload_frequency)

    # Determine whether file truncation should be performed
    is_truncate_enabled = truncate_size != 0
    log.info(f"Beginning file upload every {upload_frequency} seconds. Truncation is set to {is_truncate_enabled}")

    # Keep track of the current file hash
    current_hash = ""
    # Keep track of a unique file timestamp if truncation is enabled
    file_timestamp = f"-{int(time.time())}" if is_truncate_enabled else ""
    while True:
        try:
            new_hash = get_hash(local_path)
            log.debug(f"Current Hash: {current_hash}, New Hash: {new_hash}")
            # If file has changed, upload the new file
            if current_hash != new_hash:
                upload(local_path, f"{remote_path}{file_timestamp}", s3_bucket_name,
                       s3_bucket_region, s3_access_key_id, s3_access_key_secret)

            if is_truncate_enabled:
                # Check if the log file is too large
                file_size = os.path.getsize(local_path)
                log.debug(f"Current File Size: {file_size}")

                if file_size > truncate_size:
                    # Truncate the file
                    log.info("File has reached maximum size. Clearing...")
                    with open(local_path, "w+"):
                        log.info(f"File has been truncated. New Size: {os.path.getsize(local_path)}")
                    file_timestamp = f"-{int(time.time())}"
        except Exception as e:
            log.error(f"Unhandled exception occurred: {e}", exc_info=True)
        time.sleep(upload_frequency)


if __name__ == "__main__":
    main()
