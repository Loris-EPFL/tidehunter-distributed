#!/usr/bin/env python3
"""One verified evidence upload and one campaign ledger for every driver.

`campaign_manifest.evidence_key` decides the layout: compact summaries under
`<prefix>/<stage>/evidence/<cell>/attempt-<n>/`, per-sample files under
`<prefix>/<stage>/raw/<cell>/attempt-<n>/`. A cell passes only in this order:
every object is uploaded with `ifGenerationMatch=0` and read back by size and
SHA-256, `Ledger.mark_passed(..., store=ObjectStoreLedger)` re-verifies every
key, and `gcs_ledger.save_ledger` writes `<prefix>/campaign-ledger.json` with a
generation precondition, merging a concurrent copy. `--allow-local-ledger` is
the one local-rehearsal escape and never uploads.
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'sui-checkpoint'))
import campaign_ledger as ledger_module
import campaign_manifest
import gcp_api
import gcs_ledger


def open_store(bucket_prefix, account=campaign_manifest.ACCOUNT):
    """Return (store, bucket, prefix) through the delegated guest identity."""
    bucket, prefix = campaign_manifest.parse_bucket_prefix(bucket_prefix)
    api = gcp_api.GoogleApi(campaign_manifest.PROJECT)
    if account:
        api = api.impersonated(account)
    return api.objects(bucket), bucket, prefix


def artifact(path, name):
    """One artifact row: local path, object name, size and SHA-256."""
    path = Path(path)
    return dict(path=str(path), name=name, size=path.stat().st_size,
                sha256=ledger_module.sha256_file(path))


class Evidence:
    """Verified, attempt-scoped upload of one cell's artifacts."""

    def __init__(self, store, prefix):
        self.store = store
        self.prefix = prefix
        self.ledger_store = gcs_ledger.ObjectStoreLedger(store)
        self.ledger_key = campaign_manifest.ledger_key(prefix)

    def key(self, stage, cell_id, name, attempt=1):
        return campaign_manifest.evidence_key(self.prefix, stage, cell_id, name, attempt)

    def upload(self, artifacts, *, stage, cell_id, attempt=1):
        """Upload each artifact once, then verify the stored bytes.

        An object that already exists must hold the same bytes; a different
        object is a hard failure, never an overwrite. The local file is read
        again after the upload and must keep its hash.
        """
        receipts = []
        for artifact in artifacts:
            path = Path(artifact['path'])
            data = path.read_bytes()
            digest = ledger_module.sha256_bytes(data)
            if len(data) != artifact['size'] or digest != artifact['sha256']:
                raise ValueError(f'evidence changed before upload: {path}')
            key = self.key(stage, cell_id, artifact['name'], attempt)
            try:
                receipt = self.store.put(key, data, if_generation_match=0,
                                         metadata={'kind': 'cell-evidence',
                                                   'cell': cell_id})
                generation = int(receipt['generation'])
                if (int(receipt.get('size', -1)) != len(data)
                        or receipt.get('metadata', {}).get('sha256') != digest):
                    raise ValueError(f'evidence upload receipt differs: {key}')
            except gcp_api.ApiError as error:
                if error.status not in (409, 412):
                    raise
                head = self.store.head(key)
                if (int(head.get('size', -1)) != len(data)
                        or head.get('metadata', {}).get('sha256') != digest):
                    raise ValueError(f'uploaded evidence differs: {key}') from None
                generation = int(head['generation'])
            if ledger_module.sha256_file(path) != digest:
                raise ValueError(f'local evidence mirror changed during upload: {path}')
            artifact['key'] = key
            receipts.append(dict(key=key, path=str(path), size=len(data),
                                 sha256=digest, generation=generation))
        return receipts

    def save(self, ledger):
        generation = gcs_ledger.save_ledger(self.store, ledger, self.ledger_key)
        payload = ledger.to_bytes()
        head = self.ledger_store.head(self.ledger_key)
        if (head['size'] != len(payload)
                or head['sha256'] != ledger_module.sha256_bytes(payload)
                or head['generation'] != generation):
            raise ValueError('uploaded ledger differs from the local ledger')
        return generation, head

    def merge(self, ledger):
        """Read the campaign ledger so a resume on another host starts from it."""
        try:
            data, _ = self.ledger_store.get(self.ledger_key)
        except KeyError:
            return ledger
        merged = ledger.merge(ledger_module.Ledger.from_bytes(data))
        merged.path = ledger.path
        return merged

    def persist(self, ledger, entry, artifacts):
        """Pass the cell only after its evidence and the ledger are verified."""
        receipts = self.upload(artifacts, stage=entry['stage'],
                               cell_id=entry['id'], attempt=entry['attempts'])
        ledger.mark_passed(entry, artifacts, store=self.ledger_store)
        generation, head = self.save(ledger)
        local = (dict(path=str(ledger.path),
                      sha256=ledger_module.sha256_file(ledger.path))
                 if ledger.path is not None else None)
        return dict(cell_id=entry['id'], stage=entry['stage'], status='passed',
                    attempt=entry['attempts'], uploaded=True,
                    ledger=dict(key=self.ledger_key, generation=generation,
                                size=head['size'], sha256=head['sha256'], local=local),
                    evidence=receipts)


def pass_local(ledger, entry, artifacts):
    """`--allow-local-ledger`: a labelled local rehearsal pass, never an upload."""
    ledger.mark_passed(entry, artifacts)
    ledger.save()
    return dict(cell_id=entry['id'], stage=entry['stage'], status='passed',
                attempt=entry['attempts'], uploaded=False,
                ledger=dict(local=str(ledger.path), uploaded=False))
