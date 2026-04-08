"""
This script compares expected shipment references (from CSV exports) 
against files actually uploaded to SFTP server.

Workflow:
    1. Read the latest CSV export (.txt) from 'csv_extractor.py'.
    2. Read the two latest SFTP files (.txt), compute the diff
       to obtain only newly uploaded filenames.
    3. Clean SFTP paths to extract bare shipment references.
    4. Merge new SFTP data with previously recorded pre-upload SFTP data.
    5. Merge new CSV data with file missed from last time.
    6. Compare CSV with SFTP.
    7. Export results with timestamp
"""

import logging
import os
from pathlib import Path
from datetime import datetime
from config import setup_logging, CURRENT_DATE_TIME, CSV_DIR, SFTP_DIR,\
RESULT_DIR, SURPLUS_DIR

# Initialize shared logging
setup_logging()

# =============================================================================
# Functions
# =============================================================================

class FileComparator:

    def __init__(self, csv_dir: Path = CSV_DIR, sftp_dir: Path = SFTP_DIR,
                result_dir: Path = RESULT_DIR, surplus_dir: Path = SURPLUS_DIR):

        self.csv_dir = csv_dir
        self.sftp_dir = sftp_dir
        self.result_dir = result_dir
        self.surplus_dir = surplus_dir
        self.result_list = []
        self.insequence_list = [] # Store data that havent upload at csv but in sftp
        self.latest_sftp_file = None  # Track latest SFTP file for post-processing rename


    def read_latest_txt(self, dir_path: Path, sftp: bool = False) -> list:
        """
        Read Ship_Ref from the most recent .txt file(s).
        When used for csv data, reads latest file.
        When used for sftp data, reads two latest files and return
        new Ship_Ref found.
        """

        logging.debug(f"Reading directory {dir_path}")
        file_type = "sftp" if sftp else "csv"

        # Ensure the path exists & is a directory
        if not dir_path.exists() or not dir_path.is_dir():
            raise FileNotFoundError("Directory not found")

        txt_files = list(dir_path.glob("*.txt"))
        logging.debug(f"{len(txt_files)} .txt files found")

        # Nothing inside the directory
        if not txt_files:
            raise FileNotFoundError("Didn't find any .txt files")

        # If only one file, everything is considered "new added"
        if sftp and len(txt_files) < 2:
            logging.warning("All sftp in past 10 days are new uploaded.")

        files_dict = {}
        for file in txt_files:

            # Get file modification timestamp and convert to Datetime object
            timestamp = os.path.getmtime(file)
            datestamp = datetime.fromtimestamp(timestamp)

            # Utilise setdefault() to ensure datestamp didnt overwrite
            files_dict.setdefault(file, datestamp)

        # Sort in descending order based on value(datestamp)
        files_sorted = sorted(files_dict.items(), key=lambda item: item[1], reverse=True)

        logging.info(f"Latest {file_type} file found: {files_sorted[0][0].name}")

        # For csv, return one list
        if not sftp:
            try:
                with open(files_sorted[0][0], 'r', encoding='utf-8') as data:
                    # Remove '\n' in list()
                    return data.read().splitlines()
                
            except Exception as e:
                raise SystemExit(f"Couldn't read {files_sorted[0][0].name} | {e}")

        # For sftp, get new data upload
        else:
            # Check if latest SFTP file was already processed (has HHMMSS suffix)
            latest_stem = files_sorted[0][0].stem
            parts = latest_stem.split("_", 1)
            if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 6:
                raise SystemExit(
                    f"SFTP file '{files_sorted[0][0].name}' has already been processed.\n"
                    "Please add a new SFTP snapshot before running again."
                )
            self.latest_sftp_file = files_sorted[0][0]

            logging.info(f"Second {file_type} file found: {files_sorted[1][0].name}")

            try:
                with open(files_sorted[0][0], 'r', encoding='utf-8') as data:
                    new_list = data.read().splitlines()
                    new_list = self.filter_parent_path(new_list)
            except Exception as e:
                raise SystemExit(f"Couldn't read {files_sorted[0][0].name} | {e}")

            try:
                with open(files_sorted[1][0], 'r', encoding='utf-8') as data:
                    old_list = data.read().splitlines()
                    old_list = self.filter_parent_path(old_list)
            except Exception as e:
                raise SystemExit(f"Couldn't read {files_sorted[1][0].name} | {e}")

            new_data = [line for line in new_list if line not in set(old_list)]
            logging.info(f"Found {len(new_data)} new files uploaded")

            return new_data


    def filter_parent_path(self, list: list) -> list:
        """
        Read latest data and clean each path into a bare Ship_Ref.

        First, strip the leading directory path (e.g. /opt/sftp/...),
        then remove the date-time suffix before the first '_'
        (e.g. REF123_01012024120000.pdf → REF123).
        """

        cleaned_data = []

        logging.debug("Cleaning sftp file path")
        for ship_ref in list:

            # Remove directory path to get file name
            filename = Path(ship_ref).name

            # Use string slicing to remove date suffix
            index = filename.find("_")

            if index != -1:
                cleaned_data.append(filename[:index])
            
            else:
                # Add file name directly if no '_' found
                cleaned_data.append(filename)

        if not cleaned_data:
            logging.warning("None valid ship_ref is read")

        return cleaned_data


    def read_last_record(self, dir_path: Path, label: str = "") -> list:
        """
        Read the most recently modified .txt file.

        Used to retrieve the previous result or surplus list so that
        still-relevant data can be carried forward.
        """

        txt_files = list(dir_path.glob("*.txt"))

        if not txt_files:
            return []

        latest = max(txt_files, key=os.path.getmtime)
        logging.debug(f"Reading previous {label} file: {latest.name}")

        try:
            with open(latest, 'r', encoding='utf-8') as f:
                return f.read().splitlines()

        except Exception as e:
            logging.warning(f"Could not read {latest.name} | {e}")
            return []


    def mark_sftp_processed(self):
        """
        Rename the latest SFTP file with a full timestamp (HHMMSS)
        to mark it as processed, preventing repeated consumption
        of the same SFTP data.

        e.g. 07042026_AM.txt → 07042026_145856.txt
        """

        if not self.latest_sftp_file or not self.latest_sftp_file.exists():
            return

        date_part = self.latest_sftp_file.stem.split("_")[0]
        time_part = CURRENT_DATE_TIME.split("_")[1]
        new_name = f"{date_part}_{time_part}.txt"
        new_path = self.latest_sftp_file.parent / new_name

        if new_path.exists():
            logging.warning(f"Cannot rename: {new_name} already exists")
            return

        self.latest_sftp_file.rename(new_path)
        logging.info(f"SFTP file processed: {self.latest_sftp_file.name} → {new_name}")
        print(f"SFTP file renamed: {self.latest_sftp_file.name} → {new_name}")


    # def carry_forward_missing(self, sftp_set: set) -> list:
    #     """
    #     Return Ship_Ref from the previous result file that are still 
    #     absent from the current SFTP set.

    #     If Ship_Ref have been uploaded, it will be dropped and will not 
    #     appear in the new result file.
    #     """

    #     prev_missing = self.read_last_record(self.result_dir, "result")

    #     if not prev_missing:
    #         logging.debug("No previous result file found, nothing to carry forward")
    #         return []

    #     still_missing = [ref for ref in prev_missing if ref not in sftp_set]

    #     return still_missing


    def display_result_in_terminal(self):
        """
        Display Ship_Ref that are missing from SFTP.
        """

        if not self.result_list:
            raise SystemExit("All files have been upload successfully.")

        print("─" * 20)
        print(f"{'No':<4} {'Ship_Ref ':^16}")
        print("─" * 20)

        for index, file in enumerate(self.result_list, 1):
            print(f"{index:<4} {file:<16}")

        print("─" * 20)


    def start(self):
        """
        1. Load the latest CSV reference list and cleaned SFTP uploads.
        2. Merge SFTP set with (pre-uploads found in SFTP).
        3. Identify files missing from SFTP, merge with still-missing
           files carried forward from previous file.
        4. Identify SFTP file not yet update in the CSV, merge with 
           previous (pre-upload) that still not updated.
        5. Display missing files in the terminal and export both lists.
        """

        # New data uploaded in SFTP
        sftp_data = self.read_latest_txt(self.sftp_dir, True)

        # Data that uploaded in SFTP but not recorded in csv
        pre_upload = self.read_last_record(self.surplus_dir, "surplus")
        sftp_combined = list(dict.fromkeys(list(pre_upload) + list(sftp_data)))
        sftp_set = set(sftp_combined)


        # New data updated in csv
        csv_data = self.read_latest_txt(self.csv_dir, False)
        # Last missing data, check again
        last_missing_sftp = self.read_last_record(self.result_dir, "result")
        # Use dictionary to remove duplicates (key is unique), then convert to list
        csv_combined = list(dict.fromkeys(list(csv_data) + list(last_missing_sftp)))
        # Separate set ONLY for O(1) lookup — doesn't need order
        csv_set = set(csv_combined)

        # Files haven't been upload to SFTP
        file_missing = [f for f in csv_combined if f not in sftp_set]
        self.result_list = list(dict.fromkeys(file_missing))

        # Files hasnt recorded in csv
        wait_update = [f for f in sftp_combined if f not in csv_set]
        self.insequence_list = list(dict.fromkeys(wait_update))
        logging.info(f"{len(self.result_list)} files haven't upload to SFTP")
        logging.info(f"{len(self.insequence_list)} file hasn't updated in csv")

        self.display_result_in_terminal()

        self.export_result(self.result_list, self.result_dir, "Results")
        self.export_result(self.insequence_list, self.surplus_dir, "Pre-uploads")

        # Lock: rename SFTP file to prevent reprocessing same data
        self.mark_sftp_processed()
        

    def export_result(self, output_data: list, path: Path, title: str = ""):
        """
        Export result to .txt file with timestamp.

        If latest output file is same as the current data, the system will
        not create a new .txt file. This reduce duplicate files created.
        """

        # Check latest .txt file to avoid redundant data export
        txt_files = list(path.glob("*.txt"))

        if txt_files:
            latest_txt = max(txt_files, key=os.path.getmtime)

            try:
                with open(latest_txt, 'r', encoding='utf-8') as file:
                    past_list = file.read().splitlines()

                if past_list == output_data:
                    logging.warning(f"{title} same as latest file {latest_txt.name}, no file created")
                    return

            except Exception as e:
                logging.warning(f"Could not read {latest_txt.name} | {e}")

        # Define output path
        result_file = path / f"{CURRENT_DATE_TIME}.txt"

        # Writing results
        with open(result_file, 'w', encoding='utf-8') as file:
            for ship_ref in output_data:
                file.write(f"{ship_ref}\n")

        logging.info(f"{title} have been uploaded and renamed as {CURRENT_DATE_TIME}.txt")
        print(f"{title} saved as {CURRENT_DATE_TIME}.txt")


# -------------------------------------------------
# Main Entry Point
# -------------------------------------------------

if __name__ == "__main__":

    logging.info("sync_validator.py program started")

    try:        
        comparator = FileComparator()
        comparator.start()
        
    except FileNotFoundError as e:
        logging.error(e)
        print(e)

    except SystemExit as e:
        logging.error(e)
        print(e)

    except Exception as e:
        logging.error(e)
        print(e)

    finally:
        logging.info("sync_validator.py program ended")