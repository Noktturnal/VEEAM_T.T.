import argparse
import hashlib
import time
import os
import shutil
import logging


def parse_arguments():
    parser = argparse.ArgumentParser(description='Synchronize a source with replica.')
    parser.add_argument("source", type=str, help="Path to the source directory.")
    parser.add_argument("replica", type=str, help="Path to the replica directory.")
    parser.add_argument("interval", type=int, help="Interval between synchronization in seconds.")
    parser.add_argument("log_file", type=str, help="Path to the log file.")
    return parser.parse_args()


def setup_logging(log_file):
    log_file_dir = os.path.dirname(log_file)
    if not os.path.exists(log_file_dir):
        try:
            os.makedirs(log_file_dir, exist_ok=True)
            print(f"Log directory created: {log_file_dir}")
        except PermissionError:
            print(f"Permission denied: Cannot create directory {log_file_dir}")
            raise
        except Exception as e:
            print(f"Error creating directory {log_file_dir}: {e}")
            raise

    try:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                            handlers=[logging.FileHandler(log_file),
                                      logging.StreamHandler()])
        logging.info(f"Logging initialized. Log file: {log_file}")
    except Exception as e:
        print(f"Error initializing logging: {e}")
        raise


def compute_md5(file_path):
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
    except FileNotFoundError:
        logging.error(f"File not found for MD5 computation: {file_path}")
    except Exception as e:
        logging.error(f"Error computing MD5 for {file_path}: {e}")
    return hash_md5.hexdigest()


def sync_folders(source, replica):
    if not os.path.exists(replica):
        try:
            os.makedirs(replica)
            logging.info(f"Replica directory created: {replica}")
        except PermissionError:
            logging.error(f"Permission denied: Cannot create directory {replica}")
            raise
        except Exception as e:
            logging.error(f"Error creating directory {replica}: {e}")
            raise

    for root, dirs, files in os.walk(source):
        relative_path = os.path.relpath(root, source)
        replica_root = os.path.join(replica, relative_path)

        for dir_name in dirs:
            replica_dir = os.path.join(replica_root, dir_name)
            if not os.path.exists(replica_dir):
                try:
                    os.makedirs(replica_dir)
                    logging.info(f"Directory created: {replica_dir}")
                except Exception as e:
                    logging.error(f"Error creating directory {replica_dir}: {e}")

        for file_name in files:
            source_file = os.path.join(root, file_name)
            replica_file = os.path.join(replica_root, file_name)
            try:
                if not os.path.exists(replica_file) or (compute_md5(source_file) != compute_md5(replica_file)):
                    shutil.copy2(source_file, replica_file)
                    logging.info(f"Synchronizing {source_file} to {replica_file}")
            except Exception as e:
                logging.error(f"Error synchronizing file {source_file} to {replica_file}: {e}")

    for root, dirs, files in os.walk(replica):
        relative_path = os.path.relpath(root, replica)
        source_root = os.path.join(source, relative_path)

        for file_name in files:
            replica_file = os.path.join(root, file_name)
            source_file = os.path.join(source_root, file_name)
            if not os.path.exists(source_file):
                try:
                    os.remove(replica_file)
                    logging.info(f"File removed: {replica_file}")
                except Exception as e:
                    logging.error(f"Error removing file {replica_file}: {e}")

        for dir_name in dirs:
            replica_dir = os.path.join(root, dir_name)
            source_dir = os.path.join(source_root, dir_name)
            if not os.path.exists(source_dir):
                try:
                    shutil.rmtree(replica_dir)
                    logging.info(f"Directory removed: {replica_dir}")
                except Exception as e:
                    logging.error(f"Error removing directory {replica_dir}: {e}")


def main():
    args = parse_arguments()
    setup_logging(args.log_file)

    source = args.source
    replica = args.replica
    interval = args.interval

    logging.info(f"Starting synchronization. Source: {source}, Replica: {replica}, Interval: {interval} seconds")

    while True:
        sync_folders(source, replica)
        logging.info("Synchronization complete. Waiting for next interval...")
        time.sleep(interval)


if __name__ == '__main__':
    main()
