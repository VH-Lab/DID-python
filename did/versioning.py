from __future__ import annotations
import did.types as T
import json
from blake3 import blake3

def hash_document(document):
    serialized_data = bytes(json.dumps(document.data), 'utf8')
    return blake3(serialized_data).hexdigest()

def hash_snapshot(snapshot_id:str, document_hashes: T.List[str]):
    snapshot_id = bytes(snapshot_id)
    document_hashes = [bytes(doc_hash, 'utf8') for doc_hash in document_hashes]
    hasher = blake3()
    hasher.update(snapshot_id)
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