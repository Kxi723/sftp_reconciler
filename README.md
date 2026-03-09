# File Comparator

> A handy Python script to reconcile expected files (from Excel) against actual files uploaded to an SFTP server.

## What it Does
This tool compares two `.txt` lists of filenames to find discrepancies. It automatically:
1. **Cleans up** messy SFTP filenames (e.g., stripping directory paths and `_DDMMYYYY123456.pdf` date suffixes).
2. **Cross-checks** the cleaned SFTP list against your expected Excel list.
3. Outputs **Missing Files** (in Excel but not SFTP) to the `Result/` folder.
4. Outputs **Extra Files** (in SFTP but not Excel) to the `Error/` folder.

## Folder Structure
The script relies on a specific directory structure. **Important:** Only keep exactly ONE `.txt` file in the input folders at a time.
* `Excel/` - Put your expected filename list here (`.txt`).
* `SFTP/` - Put your actual SFTP filename list here (`.txt`).
* `Result/` - Generated list of missing files will be saved here.
* `Error/` - Generated list of extra/unexpected files will be saved here.
* `Log/` - Check here for detailed execution logs.

## How to Use
1. Drop your expected list into the `Excel/` folder.
2. Drop your actual list into the `SFTP/` folder.
