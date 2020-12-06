import json
from blake3 import blake3

def hash_document(document):
    serialized_data = bytes(json.dumps(document.data), 'utf8')
    return blake3(serialized_data).hexdigest()