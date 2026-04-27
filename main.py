# -*- coding: utf-8 -*-
"""
Job Analyzer — uses Gemini AI to analyze individual job postings.

Reads an Excel file produced by job-scraper, runs Gemini AI on each job,
and writes structured analysis fields back to a new Excel file.

Supports:
  - Checkpoint/resume (safe to interrupt and restart)
  - Single-file or folder-batch mode
  - Atomic Excel saves with retry on file-lock

Usage:
  1. Edit config section below — set TARGET_EXCEL_FILE or TARGET_FOLDER
  2. python main.py
"""

import os
import sys
import json
import shutil
import time
import re
from pathlib import Path
from typing import Optional
from datetime import datetime
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.job_data import JobData, JobDataCollection
from ai_analysis.gemini_client import GeminiClient
from ai_analysis.batch_processor import BatchProcessor
from ai_analysis.prompts import PromptManager

# =============================================================================
# CONFIG — edit here before running
# =============================================================================

# Set to True to process all .xlsx files in TARGET_FOLDER
BATCH_MODE = False

# Single-file mode
TARGET_EXCEL_FILE = r""  # e.g. r"C:\data\jobs_singapore.xlsx"

# Batch mode
TARGET_FOLDER = r""  # e.g. r"C:\data\output\run_2024_01"

# Gemini API key — prefer setting via .env file
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-2.5-flash"

# Rate limits
AI_RATE_LIMIT_PER_MINUTE = 60
AI_DAILY_LIMIT = 10000

# Checkpoint: save Excel every N jobs
CHECKPOINT_INTERVAL = 10

# Retry on Excel file lock
EXCEL_SAVE_RETRIES = 3
EXCEL_SAVE_RETRY_DELAY = 2

# Prompt template name (matches ai_analysis/prompt_templates/<name>.txt)
PROMPT_TEMPLATE = "default"

# Fields written to output Excel (prefixed with "AI_")
AI_ANALYSIS_FIELDS = [
    "job_category",
    "seniority_level",
    "years_experience_required",
    "education_required",
    "remote_policy",
    "company_stage",
]

# =============================================================================
# HELPERS
# =============================================================================

def _clean_excel_string(value):
    """Strip control characters rejected by openpyxl."""
    if isinstance(value, str):
        return re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]', '', value)
    return value


def _sanitize_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Apply _clean_excel_string to all string columns before writing Excel."""
    object_cols = df.select_dtypes(include=["object"]).columns
    if not len(object_cols):
        return df
    df = df.copy()
    for col in object_cols:
        df[col] = df[col].map(_clean_excel_string)
    return df


def excel_row_to_jobdata(row: pd.Series, row_index: int) -> JobData:
    """Convert one Excel row to a JobData object."""
    posted_date = None
    if pd.notna(row.get('Posted Date')):
        try:
            posted_date = pd.to_datetime(row['Posted Date']).to_pydatetime()
        except Exception:
            pass

    ai_analysis = None
    ai_analyzed = False
    if any(pd.notna(row.get(f'AI_{field}')) for field in AI_ANALYSIS_FIELDS):
        ai_analyzed = True
        ai_analysis = {}
        for field in AI_ANALYSIS_FIELDS:
            value = row.get(f'AI_{field}')
            if pd.notna(value):
                if isinstance(value, str):
                    if value.startswith('[') and value.endswith(']'):
                        try:
                            ai_analysis[field] = json.loads(value)
                        except Exception:
                            ai_analysis[field] = [v.strip() for v in value.strip('[]').split(',')]
                    elif ',' in value:
                        ai_analysis[field] = [v.strip() for v in value.split(',')]
                    else:
                        ai_analysis[field] = value
                else:
                    ai_analysis[field] = value

    return JobData(
        job_id=str(row_index),
        title=str(row.get('Job Title', '')),
        company=str(row.get('Company Name', '')),
        location=str(row.get('Location', '')),
        description=str(row.get('Job Description', '')),
        salary_range=str(row.get('Salary Range', '')),
        estimated_annual=row.get('Estimated Annual Salary') if pd.notna(row.get('Estimated Annual Salary')) else None,
        estimated_annual_usd=row.get('Estimated Annual Salary (USD)') if pd.notna(row.get('Estimated Annual Salary (USD)')) else None,
        posted_date=posted_date,
        job_url=str(row.get('Job Link', '')),
        platform=str(row.get('Platform', '')),
        requirements=str(row.get('Requirements', '')),
        company_size=str(row.get('Company Size', '')),
        job_status=str(row.get('Job Status', 'Active')),
        ai_analysis=ai_analysis,
        ai_analyzed=ai_analyzed,
    )


def update_excel_from_collection(df: pd.DataFrame, collection: JobDataCollection, output_file: str,
                                  max_retries: int = 3, retry_delay: int = 2) -> bool:
    """Write AI analysis results back to Excel using atomic temp-file replacement."""
    for job in collection:
        row_idx = int(job.job_id)
        if 0 <= row_idx < len(df):
            for field in AI_ANALYSIS_FIELDS:
                value = ""
                if job.ai_analysis:
                    value = job.ai_analysis.get(field, "")
                    if isinstance(value, list):
                        value = ", ".join(str(v) for v in value)
                    elif value is None:
                        value = ""
                    else:
                        value = str(value)
                df.at[row_idx, f'AI_{field}'] = value

    df = _sanitize_dataframe_for_excel(df)
    output_path = Path(output_file)
    temp_file = output_path.parent / f"~temp_{output_path.name}"

    for attempt in range(max_retries):
        try:
            df.to_excel(str(temp_file), index=False, engine='openpyxl')
            if temp_file.exists():
                if output_path.exists():
                    output_path.unlink()
                temp_file.rename(output_path)
                return True
        except PermissionError:
            if attempt < max_retries - 1:
                print(f"[WARNING] Excel locked, retrying in {retry_delay}s ({attempt+1}/{max_retries})...")
                time.sleep(retry_delay)
            else:
                print(f"[ERROR] Excel file locked after {max_retries} retries: {output_file}")
                if temp_file.exists():
                    temp_file.unlink()
                return False
        except Exception as e:
            print(f"[ERROR] Failed to save Excel: {e}")
            if temp_file.exists():
                temp_file.unlink()
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                return False

    return False


# =============================================================================
# CORE ANALYSIS
# =============================================================================

def analyze_excel_file(input_file: str):
    """Analyze a single Excel file with Gemini AI."""
    input_path = Path(input_file)
    if not input_path.exists():
        print(f"[ERROR] File not found: {input_file}")
        return

    output_dir = input_path.parent / f"analysis_gemini_{input_path.stem}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{input_path.stem}_ai_gemini{input_path.suffix}"
    checkpoint_dir = output_dir / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("Job Analyzer — Gemini AI")
    print("=" * 80)
    print(f"Input:      {input_path.name}")
    print(f"Output dir: {output_dir.name}")
    print(f"Output:     {output_file.name}")
    print(f"Checkpoint: every {CHECKPOINT_INTERVAL} jobs")
    print(f"Model:      {GEMINI_MODEL}")
    print("=" * 80)
    print()

    try:
        df = pd.read_excel(input_file, engine='openpyxl')
    except Exception as e:
        print(f"[ERROR] Failed to read Excel: {e}")
        return

    print(f"Total rows: {len(df)}\n")

    if not output_file.exists():
        print("Creating output file copy...")
        shutil.copy2(input_file, output_file)
    else:
        print(f"Output file exists, resuming: {output_file.name}")
        df = pd.read_excel(output_file, engine='openpyxl')

    for field in AI_ANALYSIS_FIELDS:
        if f'AI_{field}' not in df.columns:
            df[f'AI_{field}'] = ""

    if not GEMINI_API_KEY:
        print("[ERROR] GEMINI_API_KEY not set — add it to .env or set it in main.py")
        return

    try:
        gemini_client = GeminiClient(
            api_key=GEMINI_API_KEY,
            model=GEMINI_MODEL,
            rate_limit_per_minute=AI_RATE_LIMIT_PER_MINUTE,
            daily_limit=AI_DAILY_LIMIT,
        )
    except Exception as e:
        print(f"[ERROR] Failed to initialize Gemini client: {e}")
        import traceback
        traceback.print_exc()
        return

    prompt_manager = PromptManager()

    # Use a stable batch_id so checkpoints survive across runs
    batch_id = input_path.stem

    # Remove stale checkpoint files from previous batch IDs
    expected_checkpoint = checkpoint_dir / f"{batch_id}_checkpoint.json"
    for old in checkpoint_dir.glob("*_checkpoint.json"):
        if old != expected_checkpoint:
            print(f"[Cleanup] Removing old checkpoint: {old.name}")
            old.unlink()

    if expected_checkpoint.exists():
        print(f"[Checkpoint] Will resume from: {expected_checkpoint.name}")
    else:
        print(f"[Checkpoint] Starting fresh: {expected_checkpoint.name}")
    print()

    # Load all rows into a collection
    print("Loading jobs from Excel...")
    collection = JobDataCollection()
    for row_idx, row in df.iterrows():
        collection.add(excel_row_to_jobdata(row, row_idx), deduplicate=False)
    print(f"Loaded: {len(collection)} jobs\n")

    to_process = sum(1 for job in collection if not job.ai_analyzed)
    already_done = len(collection) - to_process
    print(f"Already analyzed: {already_done}")
    print(f"To analyze:       {to_process}\n")

    def save_excel_callback() -> bool:
        return update_excel_from_collection(
            df, collection, str(output_file),
            max_retries=EXCEL_SAVE_RETRIES,
            retry_delay=EXCEL_SAVE_RETRY_DELAY,
        )

    batch_processor = BatchProcessor(
        gemini_client=gemini_client,
        prompt_manager=prompt_manager,
        checkpoint_dir=str(checkpoint_dir),
        checkpoint_interval=CHECKPOINT_INTERVAL,
        save_callback=save_excel_callback,
    )

    last_report = 0
    success_in_window = 0
    start_time = time.time()

    def progress_callback(processed: int, total: int, job: JobData):
        nonlocal last_report, success_in_window
        success_in_window += 1
        if processed - last_report >= 10 or processed == total:
            elapsed = time.time() - start_time
            speed = processed / elapsed if elapsed > 0 else 0
            print(f"  [{processed}/{total}] {success_in_window} success | {speed:.2f} jobs/sec | {elapsed/60:.1f} min")
            last_report = processed
            success_in_window = 0

    print("Starting AI analysis...")
    print("-" * 80)

    try:
        batch_processor.process_collection(
            collection=collection,
            template_name=PROMPT_TEMPLATE,
            batch_id=batch_id,
            progress_callback=progress_callback,
            resume=True,
        )
    except KeyboardInterrupt:
        print("\n[Interrupted] Progress saved to checkpoint")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\nSaving final results...")
    if not update_excel_from_collection(df, collection, str(output_file),
                                         max_retries=EXCEL_SAVE_RETRIES,
                                         retry_delay=EXCEL_SAVE_RETRY_DELAY):
        print("[WARNING] Final save failed — data is safe in checkpoint")

    elapsed = time.time() - start_time
    analyzed = sum(1 for job in collection if job.ai_analyzed)
    stats = gemini_client.stats

    print("\n" + "=" * 80)
    print("Done!")
    print("=" * 80)
    print(f"  Total rows:    {len(df)}")
    print(f"  Analyzed:      {analyzed}")
    print(f"  Time:          {elapsed/60:.1f} min")
    print(f"  API calls:     {stats['request_count']}")
    print(f"  Tokens:        {stats['input_tokens']:,} in / {stats['output_tokens']:,} out")
    print(f"  Est. cost:     ${stats['estimated_cost_usd']:.4f}")
    print(f"  Output file:   {output_file.name}")
    print("=" * 80)


# =============================================================================
# BATCH MODE
# =============================================================================

def batch_process_folder(folder_path: str):
    """Process all unanalyzed .xlsx files in a folder."""
    folder = Path(folder_path)
    if not folder.exists():
        print(f"[ERROR] Folder not found: {folder_path}")
        return

    xlsx_files = []
    for file in folder.glob("*.xlsx"):
        if "_ai_perplexity" in file.stem or "_ai_gemini" in file.stem:
            continue
        if file.name.startswith("~$"):
            continue
        result_file = folder / f"analysis_gemini_{file.stem}" / f"{file.stem}_ai_gemini{file.suffix}"
        if result_file.exists():
            print(f"  [SKIP] {file.name} — already analyzed")
            continue
        xlsx_files.append(file)

    if not xlsx_files:
        print(f"[INFO] No files to process in: {folder_path}")
        return

    print("=" * 80)
    print(f"Batch mode: {len(xlsx_files)} files to process")
    print("=" * 80)

    for i, file in enumerate(xlsx_files, 1):
        print(f"\n{'#' * 80}")
        print(f"# File {i}/{len(xlsx_files)}: {file.name}")
        print(f"{'#' * 80}\n")
        try:
            analyze_excel_file(str(file))
        except Exception as e:
            print(f"[ERROR] Failed: {file.name} — {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 80)
    print(f"Batch complete: {len(xlsx_files)} files processed")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    if BATCH_MODE:
        if not TARGET_FOLDER:
            print("[ERROR] BATCH_MODE=True but TARGET_FOLDER is empty")
            sys.exit(1)
        folder_path = TARGET_FOLDER if os.path.isabs(TARGET_FOLDER) else \
            os.path.join(os.path.dirname(os.path.abspath(__file__)), TARGET_FOLDER)
        batch_process_folder(folder_path)
    else:
        if not TARGET_EXCEL_FILE:
            print("[ERROR] BATCH_MODE=False but TARGET_EXCEL_FILE is empty")
            sys.exit(1)
        input_file = TARGET_EXCEL_FILE if os.path.isabs(TARGET_EXCEL_FILE) else \
            os.path.join(os.path.dirname(os.path.abspath(__file__)), TARGET_EXCEL_FILE)
        analyze_excel_file(input_file)
