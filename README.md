# SFTP Reconciler

> A Python toolkit that reconciles expected shipment files (from CSV exports) against files actually uploaded to an SFTP server — highlighting what's **missing** and what's uploaded **in advance**.

---

## Overview

Shipping operations export CSV manifests listing expected shipment references, while an SFTP server holds the files that have actually been uploaded. This tool bridges the two by:

1. **Extracting** newly added shipment references from CSV exports (`csv_extractor.py`).
2. **Comparing** those references against the SFTP file listing to find discrepancies (`sync_validator.py`).
3. **Exporting** timestamped result files so you can track what's missing and what arrived early.

---

## Features

- **Incremental diff** — only processes *newly added* CSV records and *newly uploaded* SFTP files, avoiding redundant work.
- **Carry-forward logic** — unresolved missing/surplus items from the last run are automatically merged into the next comparison.
- **Duplicate-safe exports** — skips writing a new output file when the data is identical to the previous run.
- **SFTP file locking** — renames processed SFTP files with a timestamp to prevent reprocessing.
- **Activity logging** — all operations are logged to `activity.log` for audit and debugging.

---

## Folder Structure

```
sftp_reconciler/
├── csv_extractor.py        # Step 1 — Extract new Ship Refs from CSV
├── sync_validator.py       # Step 2 — Compare CSV vs SFTP
├── config.py               # Shared configuration & logging setup
├── .env                    # Environment variables (git-ignored)
├── activity.log            # Runtime log (git-ignored)
│
├── evisibility_folder/     # Input  — CSV exports (*.csv) & generated .txt lists
├── sftp/                   # Input  — SFTP file listings (*.txt)
├── missing/                # Output — Ship Refs not yet uploaded to SFTP
└── in_advance/             # Output — SFTP files not yet recorded in CSV
```

> **Note:** The `evisibility_folder/`, `sftp/`, `missing/`, and `in_advance/` directories are git-ignored. `missing/` and `in_advance/` are auto-created on first run.

---

## Prerequisites

- **Python 3.8+**
- Required packages:
  ```
  pandas
  python-dotenv
  ```
  Install with:
  ```bash
  pip install pandas python-dotenv
  ```

---

## How to Use

### Step 1 — Extract new shipment references

```bash
python csv_extractor.py
```

- Place at least **two** `.csv` files (with `Ship Ref` and `POD` columns) into `evisibility_folder/`.
- The script compares the two most recent CSVs (by modification date), filters records from the past 60 days, and identifies newly added shipment references.
- A `.txt` file (e.g. `09042026_AM.txt`) is created in `evisibility_folder/` containing the new Ship Refs.

### Step 2 — Validate SFTP uploads

```bash
python sync_validator.py
```

- Place your SFTP file listing(s) as `.txt` files into `sftp/`.
- The script diffs the two most recent SFTP listings to find newly uploaded files, cleans the filenames (strips directory paths and `_DDMMYYYY_HHMMSS.pdf` suffixes), and compares them against the CSV reference list.
- Results are exported to:
  | Folder | Contains |
  | --- | --- |
  | `missing/` | Ship Refs in CSV but **not yet on SFTP** |
  | `in_advance/` | Files on SFTP but **not yet in CSV** |
- The processed SFTP file is renamed with a full timestamp to prevent re-consumption.

---

## Logging

All activity is appended to `activity.log` in the project root. Log level is set to `DEBUG` by default and can be adjusted in `config.py`.

---

## Configuration

Core paths and constants are defined in `config.py`:

| Variable | Description |
| --- | --- |
| `CSV_DIR` | Directory for CSV exports (`evisibility_folder/`) |
| `SFTP_DIR` | Directory for SFTP listings (`sftp/`) |
| `RESULT_DIR` | Output directory for missing files (`missing/`) |
| `SURPLUS_DIR` | Output directory for surplus files (`in_advance/`) |

Environment-specific values can be placed in a `.env` file (loaded via `python-dotenv`).
