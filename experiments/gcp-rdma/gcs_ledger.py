#!/usr/bin/env python3
"""Conditional GCS ledger writes through the existing delegated ObjectStore.

`ObjectStore.put(if_generation_match=...)` already issues the storage JSON API
`ifGenerationMatch` precondition and returns the new generation; `head` returns
size and the uploaded SHA-256 metadata. This module adapts that interface to
`campaign_ledger.Ledger.save_store`, translating a 409/412 into
`PreconditionFailed` so the ledger merges and retries instead of overwriting a
concurrent copy. No gcloud subprocess and no new credential path is used.
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'sui-checkpoint'))
import campaign_ledger as ledger_module
from gcp_api import ApiError, ObjectStore


class ObjectStoreLedger:
    def __init__(self, store):
        if not isinstance(store, ObjectStore):
            raise ValueError('a delegated ObjectStore is required for conditional ledger writes')
        self.store = store

    def get(self, key):
        try:
            meta = self.store.head(key)
            return self.store.get(key, generation=meta['generation']), int(meta['generation'])
        except ApiError as error:
            if error.status == 404:
                raise KeyError(key) from None
            raise

    def put(self, key, data, generation):
        try:
            receipt = self.store.put(key, data, if_generation_match=generation,
                                     metadata={'kind': 'campaign-ledger'})
        except ApiError as error:
            if error.status in (409, 412):
                raise ledger_module.PreconditionFailed(key) from error
            raise
        if int(receipt.get('size', -1)) != len(data):
            raise ValueError('ledger upload receipt size differs')
        if receipt.get('metadata', {}).get('sha256') != ledger_module.sha256_bytes(data):
            raise ValueError('ledger upload receipt hash differs')
        if not receipt.get('generation'):
            raise ValueError('ledger upload receipt lacks a generation')
        return int(receipt['generation'])

    def head(self, key):
        try:
            meta = self.store.head(key)
        except ApiError as error:
            if error.status == 404:
                raise KeyError(key) from None
            raise
        return dict(size=int(meta['size']), generation=int(meta['generation']),
                    sha256=meta.get('metadata', {}).get('sha256'))


def save_ledger(store, ledger, key):
    """Persist a ledger locally (if it has a path) and to the bucket."""
    if ledger.path is not None:
        ledger.save()
    return ledger.save_store(ObjectStoreLedger(store), key)