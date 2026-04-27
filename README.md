# Job Analyzer

Uses Gemini AI to analyze individual job postings from an Excel file produced by job-scraper. For each job, it extracts structured fields:

| Field | Values |
|---|---|
| `job_category` | ML Engineer, Data Scientist, AI Researcher, … |
| `seniority_level` | Junior, Mid, Senior, Lead, Principal, Director |
| `years_experience_required` | 0-2, 2-5, 5-8, 8-10, 10+ |
| `education_required` | Bachelor's / Master's / PhD — Required or Preferred |
| `remote_policy` | Remote, Hybrid, On-site |
| `company_stage` | Startup, Scaleup, Enterprise |

Results are written to a new Excel file with `AI_` prefixed columns.

## Features

- Checkpoint/resume: safe to interrupt and restart
- Single-file or folder-batch mode
- Atomic Excel saves with retry on file-lock
- Progress reporting every 10 jobs

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Add your Gemini API key to .env
```

## Configuration

Edit the CONFIG section at the top of `main.py`:

| Setting | Description |
|---|---|
| `BATCH_MODE` | `True` = process all .xlsx in folder; `False` = single file |
| `TARGET_EXCEL_FILE` | Path to a single Excel file (BATCH_MODE=False) |
| `TARGET_FOLDER` | Folder containing Excel files (BATCH_MODE=True) |
| `GEMINI_MODEL` | Gemini model to use (default: `gemini-2.5-flash`) |
| `CHECKPOINT_INTERVAL` | Save checkpoint every N jobs |

## Usage

```bash
python main.py
```

## Output

For each input file `jobs_singapore.xlsx`, a folder `analysis_gemini_jobs_singapore/` is created:

| File | Contents |
|---|---|
| `jobs_singapore_ai_gemini.xlsx` | Original data with AI_ columns filled in |
| `checkpoints/<name>_checkpoint.json` | Resume state |

## Prompt Customization

Edit `ai_analysis/prompt_templates/default.txt` to change the analysis prompt.
The template uses `{title}`, `{company}`, `{location}`, `{salary_range}`, and `{description}` placeholders.

## Notes

- Already-analyzed jobs (non-empty AI_ columns) are skipped automatically.
- Rate limits are configurable in `main.py` (`AI_RATE_LIMIT_PER_MINUTE`, `AI_DAILY_LIMIT`).
- Consecutive failure auto-stop: halts after 5 consecutive API failures.
