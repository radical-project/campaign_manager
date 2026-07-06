#!/usr/bin/env python3
"""
ESM2 Inference Client

Thin subclass of InferenceClient that handles PyTorch tensor serialisation:
tokenised batches (``input_ids``, ``attention_mask``) are converted to Python
lists before being sent as JSON in the HTTP POST body.
"""

from typing import Any, Optional

from ..inference_client import InferenceClient


class ESM2Client(InferenceClient):
    """
    HTTP client for remote ESM2 inference.

    Extends :class:`InferenceClient` with ESM2-specific batch serialisation:
    PyTorch token tensors stored in ``service.single_batch`` (non-streaming)
    or ``service.batch_storage`` (streaming) are converted to JSON-compatible
    lists for HTTP transport.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Cache for non-streaming mode: same tokenised batch for every request.
        self._cached_batch_data: Optional[dict[str, Any]] = None

    def _get_batch_data(self, batch_id: int) -> Optional[dict]:
        """
        Serialise the PyTorch token batch for HTTP transport.

        Non-streaming: all requests share the same pre-tokenised batch; it is
        converted once and cached.  Streaming: the per-batch tensor stored in
        ``service.batch_storage`` is converted on each call.

        Returns ``None`` when the service holds the data server-side (i.e. not
        in client mode), so the server looks up the batch by ID instead.
        """
        if not getattr(self.service, "client_mode", False):
            return None

        if not self.service.use_streaming:
            if self._cached_batch_data is None and self.service.single_batch is not None:
                self._cached_batch_data = {
                    k: v.tolist() for k, v in self.service.single_batch.items()
                }
                self.logger.info(f"[Client {self.rank}] Prepared batch data for remote requests")
            return self._cached_batch_data

        # Streaming: per-batch data
        if batch_id in self.service.batch_storage:
            return {k: v.tolist() for k, v in self.service.batch_storage[batch_id].items()}

        return None
