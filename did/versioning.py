from __future__ import annotations
import did.types as T
import json
from blake3 import blake3
from copy import deepcopy

def hash_document(document):
    doc = deepcopy(document)
    doc.data['base']['records'] = None # ignore this field
    serialized_data = bytes(json.dumps(doc.data), 'utf8')
    return blake3(serialized_data).hexdigest()

def hash_snapshot(document_hashes: T.List[str]):
    document_hashes = [bytes(doc_hash, 'utf8') for doc_hash in document_hashes]
    hasher = blake3()
    for document_hash in sorted(document_hashes):
        hasher.update(document_hash)
    return hasher.hexdigest()

def hash_commit(snapshot_hash, parent_commit_hash = None):
    snapshot_hash = bytes(snapshot_hash, 'utf8')
    hasher = blake3()
    hasher.update(snapshot_hash)
    if parent_commit_hash:
        parent_commit_hash = bytes(parent_commit_hash, 'utf8')
        hasher.update(parent_commit_hash)
    return hasher.hexdigest()