import os
import logging
from pathlib import Path
from datetime import date, datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DATE = date.today()
DATE_TIME = datetime.now()
CURRENT_DATE_TIME = DATE_TIME.strftime("%d%m%Y_%H%M%S")

# Base Directory
SCRIPT_DIR = Path(__file__).parent

# Local Directory Paths
CSV_DIR = SCRIPT_DIR / "evisibility_folder"
SFTP_DIR = SCRIPT_DIR / "sftp"
RESULT_DIR = SCRIPT_DIR / "missing"
SURPLUS_DIR = SCRIPT_DIR / "in_advance"

# Ensure Log directory exists
RESULT_DIR.mkdir(parents=True, exist_ok=True)
SURPLUS_DIR.mkdir(parents=True, exist_ok=True)

# Shared Logging Setup
def setup_logging():
    log_file_path = SCRIPT_DIR / "activity.log"
    
    logging.basicConfig(
        filename=log_file_path,
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s: %(message)s',
        filemode='a'
    )
