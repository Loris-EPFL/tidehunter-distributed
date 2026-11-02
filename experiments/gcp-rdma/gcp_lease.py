"""Single-campaign GCS lease; expiration alone never authorizes takeover."""
import json
import uuid

from gcp_api import ApiError

KEY = 'lifecycle/active-run.json'


def _encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def read(store):
    try:
        meta = store.head(KEY)
    except ApiError as exc:
        if exc.status == 404:
            return None
        raise
    return json.loads(store.get(KEY, generation=meta['generation'])), str(meta['generation'])


def acquire(store, state):
    lease = {'version': 1, 'run_id': state['run_id'], 'project_id': state['project_id'],
             'source_sha256': state['source_sha256'], 'deadline': state['deadline'],
             'state_key': 'runs/' + state['run_id'] + '/lifecycle/state.json',
             'token': uuid.uuid4().hex}
    data = _encode(lease)
    try:
        meta = store.put(KEY, data, content_type='application/json', if_generation_match=0)
    except ApiError as exc:
        # Only a lost response can be adopted, and only with our unguessable token.
        if exc.status is not None and exc.status < 500:
            raise RuntimeError('another campaign holds the active lease') from exc
        current = read(store)
        if current is None or current[0] != lease:
            raise
        return lease, current[1]
    return lease, str(meta['generation'])


def owned(store, state):
    current = read(store)
    if current is None:
        raise RuntimeError('active campaign lease is missing')
    lease, generation = current
    expected = {'run_id': state['run_id'], 'project_id': state['project_id'],
                'source_sha256': state['source_sha256'], 'deadline': state['deadline'],
                'state_key': 'runs/' + state['run_id'] + '/lifecycle/state.json'}
    if any(lease.get(k) != v for k, v in expected.items()) or not lease.get('token'):
        raise RuntimeError('active campaign lease identity mismatch')
    return lease, generation


def release(store, lease, generation):
    current = read(store)
    if current != (lease, str(generation)):
        raise RuntimeError('active campaign lease changed before release')
    try:
        store.delete(KEY, if_generation_match=generation)
    except ApiError as exc:
        if exc.status is not None and exc.status < 500:
            raise
        # A lost delete response is harmless only if this generation is gone.
        current = read(store)
        if current is not None and current == (lease, str(generation)):
            raise
