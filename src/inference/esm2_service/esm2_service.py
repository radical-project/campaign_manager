#!/usr/bin/env python3
"""
Inference Service

Multi-GPU inference service with worker pool management, batch processing,
and metrics tracking.

Contains:
- ESM2InferenceService: ESM2-specific model loading and embedding
"""

import asyncio
import os
import threading
import time
from functools import partial
from pathlib import Path
from typing import Any, Optional

import numpy as np

try:
    import torch
except ModuleNotFoundError:
    torch = None  # type: ignore[assignment]  # only needed in envs that run the service

from ..inference_service import InferenceService

# -----------------------------------------------------------------------------
# ESM2 Inference Service
# -----------------------------------------------------------------------------


class ESM2InferenceService(InferenceService):
    """
    ESM2-specific inference service for protein sequence embeddings.

    Extends BaseInferenceService with:
    - ESM2 model loading (tokenizer + model per GPU)
    - Protein sequence tokenization
    - Embedding extraction and storage
    """

    def __init__(
        self,
        config: Optional[dict[str, Any]] = None,
        devices: Optional[list[str]] = None,
        rank: int = 0,
        client_mode: bool = False,
        **kwargs,
    ):
        """Initialize ESM2 inference service."""
        super().__init__(config, devices, rank, client_mode=client_mode, **kwargs)

        # ESM2-specific imports
        from transformers.utils import logging as hf_logging

        hf_logging.disable_progress_bar()
        from transformers import EsmModel, EsmTokenizer

        self._EsmTokenizer = EsmTokenizer
        self._EsmModel = EsmModel

        # Model management
        self.model_path = Path(self.config.get("model_path", "facebook/esm2_t36_3B_UR50D"))
        self.model_initialized: dict[str, bool] = {}
        self.model_init_locks: dict[str, threading.Lock] = {}
        self.tokenizer = None
        self.models: dict[str, Any] = {}

        if client_mode:
            self._load_tokenizer()
        else:
            self._load_models()

        self.logger.info(f"[Service {self.rank}] ESM2InferenceService initialized")

    def _resolve_model_id(self):
        """Resolve model path and return (model_id, tokenizer_kwargs, cache_dir)."""
        local = self.model_path.exists() and self.model_path.is_dir()
        model_id = self.model_path if local else str(self.model_path)

        if local:
            tokenizer_kwargs = {"use_fast": True}
            cache_dir = None
        else:
            cache_dir = Path(os.getenv("PROJECT", Path.cwd())) / "cache"
            tokenizer_kwargs = {"use_fast": True, "cache_dir": cache_dir}

        return model_id, tokenizer_kwargs, cache_dir, local

    def _load_tokenizer(self):
        """Load only the ESM2 tokenizer (for client-side preprocessing)."""
        model_id, tokenizer_kwargs, _, _ = self._resolve_model_id()
        self.logger.info(f"[Service {self.rank}] Loading tokenizer only (client mode)")
        self.tokenizer = self._EsmTokenizer.from_pretrained(model_id, **tokenizer_kwargs)
        self.logger.info(f"[Service {self.rank}] Tokenizer loaded")

    def _load_models(self):
        """Load ESM2 tokenizer and one model instance per GPU."""
        t_start = time.time()

        use_cuda = any(isinstance(d, str) and d.startswith("cuda") for d in self.devices)
        dtype = torch.float16 if use_cuda else torch.float32

        model_id, tokenizer_kwargs, cache_dir, local = self._resolve_model_id()

        if local:
            self.logger.info(f"[Service {self.rank}] Loading models from local path: {model_id}")
            model_kwargs = {
                "dtype": dtype,
                "local_files_only": True,
                "low_cpu_mem_usage": True,
            }
        else:
            self.logger.info(
                f"[Service {self.rank}] Loading models '{model_id}' from Hugging Face to {cache_dir}"
            )
            model_kwargs = {
                "dtype": dtype,
                "low_cpu_mem_usage": True,
                "cache_dir": cache_dir,
            }

        # tokenizer (once)
        self.tokenizer = self._EsmTokenizer.from_pretrained(model_id, **tokenizer_kwargs)

        # models (one per device)
        for i, device in enumerate(self.devices):
            self.logger.info(
                f"[Service {self.rank}] Loading model {i + 1}/{len(self.devices)} on {device}..."
            )

            model = self._EsmModel.from_pretrained(model_id, **model_kwargs)
            model.to(device).eval()

            self.models[device] = model
            self.model_initialized[device] = False
            self.model_init_locks[device] = threading.Lock()

            self.logger.info(f"[Service {self.rank}] Model loaded successfully on {device}")

        self.logger.info(
            f"[Service {self.rank}] Loaded {len(self.models)} models across "
            f"{len(self.devices)} GPUs in {time.time() - t_start:.2f}s"
        )

    def process_batch_sync(self, batch_id: int, device: str, batch_data: Optional[dict] = None):
        """
        Run ESM2 model inference on a batch (synchronous).

        Args:
            batch_id: Batch identifier
            device: GPU device to use
            batch_data: Optional pre-tokenized batch data from client (lists of ints).
                        If provided, used directly instead of looking up from storage.
        """
        if isinstance(device, str) and device.startswith("cuda:"):
            device_id = int(device.split(":")[1])
            torch.cuda.set_device(device_id)

        with torch.no_grad():
            if batch_data is not None:
                # Remote mode: batch data sent by client
                batch = {k: torch.tensor(v).to(device) for k, v in batch_data.items()}
            elif self.use_streaming:
                batch = self.batch_storage.get(batch_id)
                if batch is None:
                    raise ValueError(f"Batch {batch_id} not found in storage")
                batch = {k: v.to(device, non_blocking=True) for k, v in batch.items()}
            else:
                device_batch = self.device_batches.get(device)
                if device_batch is not None:
                    batch = {k: v.clone() for k, v in device_batch.items()}
                else:
                    if self.single_batch is None:
                        raise ValueError(f"Batch {batch_id} requested before initialization")
                    batch = {
                        k: v.clone().to(device, non_blocking=True)
                        for k, v in self.single_batch.items()
                    }

            tokens = int(batch["attention_mask"].sum().item())
            self.logger.metrics["queue_tokens"] -= tokens
            self.logger.metrics["total_tokens"] += tokens

            model = self.models.get(device)
            if model is None:
                raise ValueError(f"No model found for device {device}")

            if not self.model_initialized.get(device, True):
                with self.model_init_locks[device]:
                    if not self.model_initialized[device]:
                        _ = model(
                            input_ids=batch["input_ids"], attention_mask=batch["attention_mask"]
                        )
                        self.model_initialized[device] = True

            reps = model(
                input_ids=batch["input_ids"], attention_mask=batch["attention_mask"]
            ).last_hidden_state

            lengths = batch["attention_mask"].sum(dim=1).tolist()

            if self.use_streaming and batch_id in self.batch_storage:
                del self.batch_storage[batch_id]

            embeddings = [reps[i, 1:length].cpu().numpy() for i, length in enumerate(lengths)]
            self.reply_store[batch_id] = embeddings
            self.processed_queue.put_nowait(batch_id)

    async def generate_batch(self) -> tuple:
        """Generate one token-bounded batch from input_queue."""
        batch_seqs = []
        batch_tokens = 0

        while True:
            seq = await self.input_queue.get()

            if seq is None:
                await self.input_queue.put(None)
                break

            seq_len = len(seq)
            if seq_len > self.max_batch_tokens:
                self.logger.warning(f"[Service {self.rank}] Skipping sequence of length {seq_len}")
                continue

            if batch_seqs and batch_tokens + seq_len > self.max_batch_tokens:
                await self.input_queue.put(seq)
                break

            batch_seqs.append(seq)
            batch_tokens += seq_len

        if not batch_seqs:
            raise StopAsyncIteration

        return_tensors = "np" if self.client_mode else "pt"
        toks = self.tokenizer(
            batch_seqs,
            return_tensors=return_tensors,
            padding=True,
            truncation=True,
            max_length=1024,
        )
        num_tokens = int(toks["attention_mask"].sum().item())
        return num_tokens, toks

    async def _result_writer(self):
        """Background task that saves embeddings to disk asynchronously."""
        while True:
            batch_id = await self.processed_queue.get()

            try:
                if batch_id is None:
                    self.logger.info(
                        f"[Service {self.rank}] Embedding writer received shutdown sentinel"
                    )
                    break

                # Benchmarking optimization: only save first N batches to disk
                # to avoid I/O bottlenecks during throughput measurement.
                # Remove or increase this limit for production use.
                if batch_id > 10:
                    self.reply_store.pop(batch_id, None)
                    continue

                if batch_id in self.reply_store:
                    reply = self.reply_store[batch_id]
                    embeddings = [np.asarray(e) for e in reply]
                    await self._save_embeddings_async(batch_id, embeddings)
                    del self.reply_store[batch_id]

                    if self.debug:
                        self.logger.debug(
                            f"[Service {self.rank}] Saved and cleaned up batch {batch_id}"
                        )
                else:
                    self.logger.warning(
                        f"[Service {self.rank}] Batch {batch_id} not found in reply_store"
                    )

            except Exception as e:
                self.logger.error(f"[Service {self.rank}] Error saving batch {batch_id}: {e}")
            finally:
                self.processed_queue.task_done()

        self.logger.info(f"[Service {self.rank}] Embedding writer stopped")

    async def _save_embeddings_async(self, batch_id: int, embeddings: list[np.ndarray]):
        """Save embeddings to disk using thread pool for async I/O."""
        loop = asyncio.get_running_loop()
        base = Path(self.results_dir, "emb")

        tasks = []
        for i, emb in enumerate(embeddings):
            path = base.with_name(f"{base.stem}_{(batch_id + i)}.npy")
            fn = partial(np.save, path, emb)
            tasks.append(loop.run_in_executor(self.save_executor, fn))

        await asyncio.gather(*tasks)
