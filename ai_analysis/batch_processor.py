# -*- coding: utf-8 -*-
"""
Batch Processor for AI job analysis.
Supports checkpointing, progress tracking, and cost control.
"""

import os
import json
import time
from typing import List, Dict, Any, Optional, Callable
from pathlib import Path
from dataclasses import dataclass, asdict

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.job_data import JobData, JobDataCollection
from ai_analysis.gemini_client import GeminiClient
from ai_analysis.prompts import PromptManager


@dataclass
class BatchCheckpoint:
    """Checkpoint state for batch processing."""
    processed_count: int = 0
    total_count: int = 0
    last_job_id: str = ""
    processed_ids: List[str] = None
    timestamp: str = ""

    def __post_init__(self):
        if self.processed_ids is None:
            self.processed_ids = []


class BatchProcessor:
    """
    Batch processor for AI analysis with checkpoint support.

    Features:
    - Checkpoint/resume capability
    - Progress tracking
    - Cost estimation
    - Rate limiting
    - Auto-stop on consecutive failures or daily limit
    """

    # Auto-stop settings
    MAX_CONSECUTIVE_FAILURES = 5  # Stop after 5 consecutive failures (conservative)

    def __init__(
        self,
        gemini_client: GeminiClient,
        prompt_manager: PromptManager = None,
        checkpoint_dir: Optional[str] = None,
        checkpoint_interval: int = 10,
        save_callback: Optional[Callable[[], bool]] = None,
    ):
        """
        Initialize batch processor.

        Args:
            gemini_client: Initialized GeminiClient
            prompt_manager: PromptManager instance (optional)
            checkpoint_dir: Directory for checkpoint files
            checkpoint_interval: Save checkpoint every N jobs
            save_callback: Optional callback to save data (e.g., Excel) before checkpoint.
                          Should return True if save successful, False otherwise.
        """
        self.client = gemini_client
        self.prompt_manager = prompt_manager or PromptManager()
        self.checkpoint_interval = checkpoint_interval
        self.save_callback = save_callback

        # Checkpoint directory
        if checkpoint_dir:
            self.checkpoint_dir = Path(checkpoint_dir)
        else:
            self.checkpoint_dir = Path(__file__).parent.parent / "output" / "checkpoints"

        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        # Current batch state
        self._current_batch_id: Optional[str] = None
        self._checkpoint: Optional[BatchCheckpoint] = None

    def process_collection(
        self,
        collection: JobDataCollection,
        template_name: str = "default",
        batch_id: Optional[str] = None,
        progress_callback: Optional[Callable[[int, int, JobData], None]] = None,
        resume: bool = True,
    ) -> JobDataCollection:
        """
        Process all jobs in a collection with AI analysis.

        Args:
            collection: JobDataCollection to process
            template_name: Name of prompt template to use
            batch_id: Unique identifier for this batch (for checkpointing)
            progress_callback: Optional callback(processed, total, job)
            resume: If True, resume from checkpoint if available

        Returns:
            Updated JobDataCollection with AI analysis results
        """
        # Generate batch ID if not provided
        if batch_id is None:
            batch_id = f"batch_{int(time.time())}"

        self._current_batch_id = batch_id

        # Load prompt template
        prompt = self.prompt_manager.get_template(template_name)
        if not prompt:
            print(f"[BatchProcessor] Template '{template_name}' not found, using default")
            prompt = self.prompt_manager.get_default_template()

        # Initialize processed_ids with jobs that are already analyzed
        processed_ids = set()
        for job in collection:
            if job.ai_analyzed:
                processed_ids.add(job.job_id or job._dedup_key)

        already_analyzed = len(processed_ids)

        # Load checkpoint if resuming (merge with existing analysis)
        if resume:
            checkpoint = self._load_checkpoint(batch_id)
            if checkpoint:
                processed_ids.update(checkpoint.processed_ids)
                if checkpoint.processed_count > already_analyzed:
                    print(f"[BatchProcessor] Resuming from checkpoint: {checkpoint.processed_count}/{checkpoint.total_count} processed")

        total = len(collection)
        processed = len(processed_ids)

        print(f"\n{'=' * 50}")
        print(f"AI Analysis Batch Processing")
        print(f"{'=' * 50}")
        print(f"  Batch ID: {batch_id}")
        print(f"  Template: {template_name}")
        print(f"  Total jobs: {total}")
        print(f"  Already processed: {processed}")
        print(f"  Remaining: {total - processed}")
        print(f"  Auto-stop: {self.MAX_CONSECUTIVE_FAILURES} consecutive failures")
        print(f"{'=' * 50}\n")

        # Auto-stop tracking
        consecutive_failures = 0
        auto_stopped = False
        stop_reason = ""

        # Process each job
        for i, job in enumerate(collection):
            # Skip already processed (from Excel or checkpoint)
            if job.ai_analyzed or job.job_id in processed_ids or job._dedup_key in processed_ids:
                continue

            # Check 1: Proactive daily limit check
            if self.client._daily_count >= self.client.daily_limit:
                auto_stopped = True
                stop_reason = "daily_limit"
                print(f"\n{'=' * 80}")
                print("AUTO-STOP: Daily API limit reached")
                print(f"{'=' * 80}")
                print(f"  Processed in this session: {processed}")
                print(f"  Total processed: {processed + already_analyzed}")
                print(f"  Daily requests: {self.client._daily_count}/{self.client.daily_limit}")
                print(f"  Progress saved to checkpoint")
                print(f"{'=' * 80}\n")
                break

            # Prepare job data for prompt
            job_data = {
                "title": job.title,
                "company": job.company,
                "location": job.location,
                "salary_range": job.salary_range or "Not specified",
                "description": job.description[:4000] if job.description else "No description",
            }

            # Analyze with AI
            result = self.client.analyze(prompt, job_data)

            if result:
                job.ai_analysis = result
                job.ai_analyzed = True
                # Only track successfully analyzed jobs
                processed += 1
                processed_ids.add(job.job_id or job._dedup_key)

                # Reset consecutive failure counter on success
                consecutive_failures = 0

                # Progress callback
                if progress_callback:
                    progress_callback(processed, total, job)
                else:
                    # Default progress output
                    print(f"[{processed}/{total}] {job.title[:40]}... [OK]")

                # Save checkpoint (with data save callback)
                if processed % self.checkpoint_interval == 0:
                    self._save_checkpoint_with_callback(batch_id, processed, total, list(processed_ids))
            else:
                # Failed analysis - don't count, will retry next time
                consecutive_failures += 1

                if not progress_callback:
                    print(f"[SKIP] {job.title[:40]}... [FAILED - will retry] (consecutive: {consecutive_failures})")

                # Check 2: Consecutive failure limit
                if consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES:
                    auto_stopped = True
                    stop_reason = "consecutive_failures"
                    print(f"\n{'=' * 80}")
                    print(f"AUTO-STOP: {self.MAX_CONSECUTIVE_FAILURES} consecutive failures detected")
                    print(f"{'=' * 80}")
                    print(f"  Processed in this session: {processed}")
                    print(f"  Total processed: {processed + already_analyzed}")

                    # Diagnose the reason
                    if self.client._daily_count >= self.client.daily_limit:
                        print(f"  Likely reason: Daily API limit reached")
                        print(f"  Daily requests: {self.client._daily_count}/{self.client.daily_limit}")
                    else:
                        print(f"  Likely reason: Persistent API errors (rate limit or quota)")
                        print(f"  Daily requests: {self.client._daily_count}/{self.client.daily_limit}")

                    print(f"  Progress saved to checkpoint")
                    print(f"{'=' * 80}\n")
                    break

        # Final checkpoint (with data save callback)
        self._save_checkpoint_with_callback(batch_id, processed, total, list(processed_ids))

        # Print summary
        self._print_summary(processed, total, auto_stopped, stop_reason)

        return collection

    def process_jobs(
        self,
        jobs: List[JobData],
        template_name: str = "default",
        batch_id: Optional[str] = None,
        progress_callback: Optional[Callable] = None,
        resume: bool = True,
    ) -> List[JobData]:
        """
        Process a list of JobData objects.

        Args:
            jobs: List of JobData to process
            template_name: Prompt template name
            batch_id: Batch identifier
            progress_callback: Progress callback
            resume: Resume from checkpoint

        Returns:
            List of processed JobData with AI analysis
        """
        collection = JobDataCollection()
        for job in jobs:
            collection.add(job, deduplicate=False)

        self.process_collection(
            collection,
            template_name=template_name,
            batch_id=batch_id,
            progress_callback=progress_callback,
            resume=resume,
        )

        return list(collection)

    def _load_checkpoint(self, batch_id: str) -> Optional[BatchCheckpoint]:
        """Load checkpoint from file."""
        checkpoint_file = self.checkpoint_dir / f"{batch_id}_checkpoint.json"

        if not checkpoint_file.exists():
            return None

        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return BatchCheckpoint(**data)
        except Exception as e:
            print(f"[BatchProcessor] Error loading checkpoint: {e}")
            return None

    def _save_checkpoint_with_callback(
        self,
        batch_id: str,
        processed: int,
        total: int,
        processed_ids: List[str],
    ):
        """
        Save checkpoint with data save callback.

        This ensures that data (e.g., Excel) is saved BEFORE checkpoint is updated.
        If data save fails, checkpoint is not updated, preventing data loss.
        """
        # First, call save callback (e.g., save Excel file)
        if self.save_callback:
            try:
                success = self.save_callback()
                if not success:
                    print(f"[BatchProcessor] Warning: Data save callback failed, checkpoint not updated")
                    return
            except Exception as e:
                print(f"[BatchProcessor] Error in save callback: {e}")
                print(f"[BatchProcessor] Checkpoint NOT updated to prevent data loss")
                return

        # If callback succeeded (or no callback), save checkpoint
        self._save_checkpoint(batch_id, processed, total, processed_ids)

    def _save_checkpoint(
        self,
        batch_id: str,
        processed: int,
        total: int,
        processed_ids: List[str],
    ):
        """Save checkpoint to file (always overwrites the same file)."""
        checkpoint = BatchCheckpoint(
            processed_count=processed,
            total_count=total,
            last_job_id=processed_ids[-1] if processed_ids else "",
            processed_ids=processed_ids,
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
        )

        # Fixed filename — always overwrites the same file so old checkpoints don't accumulate
        checkpoint_file = self.checkpoint_dir / f"{batch_id}_checkpoint.json"

        try:
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(asdict(checkpoint), f, indent=2)
        except Exception as e:
            print(f"[BatchProcessor] Error saving checkpoint: {e}")

    def _print_summary(self, processed: int, total: int, auto_stopped: bool = False, stop_reason: str = ""):
        """Print processing summary."""
        stats = self.client.stats

        print(f"\n{'=' * 50}")
        if auto_stopped:
            print("Batch Processing Stopped")
            if stop_reason == "daily_limit":
                print("(Daily API limit reached)")
            elif stop_reason == "consecutive_failures":
                print("(Consecutive failures detected)")
        else:
            print("Batch Processing Complete")
        print(f"{'=' * 50}")
        print(f"  Processed: {processed}/{total} ({processed/total*100:.1f}%)")
        print(f"  API requests: {stats['request_count']}")
        print(f"  Daily usage: {stats['daily_count']}/{stats['daily_limit']}")
        print(f"  Input tokens: {stats['input_tokens']:,}")
        print(f"  Output tokens: {stats['output_tokens']:,}")
        print(f"  Estimated cost: ${stats['estimated_cost_usd']:.4f}")
        if auto_stopped:
            print(f"\n  Note: Processing stopped early. Resume by running again.")
        print(f"{'=' * 50}")

    def clear_checkpoint(self, batch_id: str) -> bool:
        """Clear checkpoint for a batch."""
        checkpoint_file = self.checkpoint_dir / f"{batch_id}_checkpoint.json"

        if checkpoint_file.exists():
            try:
                checkpoint_file.unlink()
                return True
            except Exception as e:
                print(f"[BatchProcessor] Error clearing checkpoint: {e}")
                return False

        return True

    def list_checkpoints(self) -> List[Dict[str, Any]]:
        """List all available checkpoints."""
        checkpoints = []

        for file in self.checkpoint_dir.glob("*_checkpoint.json"):
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    data['batch_id'] = file.stem.replace('_checkpoint', '')
                    checkpoints.append(data)
            except:
                pass

        return checkpoints
