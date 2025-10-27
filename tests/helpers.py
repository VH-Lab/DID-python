import numpy as np
from did.document import Document

def make_doc_tree_invalid(rates, **options):
    """
    Makes a "tree" of documents with invalid modifications to add to a database.
    """
    value_modifier = options.get('value_modifier', 'sham')
    id_modifier = options.get('id_modifier', 'sham')
    dependency_modifier = options.get('dependency_modifier', 'sham')
    other_modifier = options.get('other_modifier', 'sham')
    remover = options.get('remover', 'sham')

    def modify_value(method, value):
        if method == 'int2str': return str(value)
        if method == 'blank int': return None
        if method == 'blank str': return ''
        if method == 'nan': return float('nan')
        if method == 'double': return value + 0.5
        if method == 'too negative': return -2**31
        if method == 'too positive': return 2**31 - 1
        return value

    def modify_id(method, id_):
        if method == 'substring': return id_[:32]
        if method == 'replace_underscore': return id_.replace('-', 'a', 1)
        if method == 'add': return id_ + 'a'
        if method == 'replace_letter_invalid1': return '*' + id_[1:]
        if method == 'replace_letter_invalid2': return "'" + id_[1:]
        return id_

    def modify_dependency(method, doc_struct):
        if 'depends_on' in doc_struct['document_properties']:
            if method == 'invalid id':
                doc_struct['document_properties']['depends_on'][0]['value'] = 'abcdefg'
            elif method == 'invalid name':
                doc_struct['document_properties']['depends_on'][0]['name'] = 'abcdefg'
        return doc_struct

    def modify_other_fields(method, doc_struct):
        # This is a simplified version of the Matlab code.
        # A more complete implementation would handle all cases.
        if method == 'invalid class name':
            doc_struct['document_properties']['document_class']['class_name'] = 'abcdefg'
        return doc_struct

    def remove_field(method, doc_struct):
        if not doc_struct or 'document_properties' not in doc_struct:
            return doc_struct

        keys = method.split('.')
        d = doc_struct['document_properties']

        for key in keys[:-1]:
            if isinstance(d, dict) and key in d:
                d = d[key]
            else:
                return doc_struct  # Parent key not found

        if isinstance(d, dict) and keys[-1] in d:
            del d[keys[-1]]

        return doc_struct

    num_a = np.random.poisson(rates[0])
    num_b = np.random.poisson(rates[1])
    num_c = np.random.poisson(rates[2])

    if dependency_modifier != 'sham' and num_c == 0:
        num_c = 1 # Ensure at least one doc with dependencies is created

    if num_a + num_b + num_c == 0:
        num_a = 1

    docs = []
    node_names = []
    ids_a = []
    ids_b = []
    ids_c = []

    counter = 1

    for _ in range(num_a):
        doc = Document('demoA')
        doc_struct = {'document_properties': doc.document_properties}

        doc_struct['document_properties']['demoA']['value'] = modify_value(value_modifier, counter)
        doc_struct['document_properties']['base']['id'] = modify_id(id_modifier, doc.id())
        doc_struct = modify_other_fields(other_modifier, doc_struct)
        doc_struct = remove_field(remover, doc_struct)

        docs.append(Document(doc_struct['document_properties']))
        node_names.append(str(counter))
        if 'id' in doc_struct['document_properties'].get('base', {}):
            ids_a.append(doc_struct['document_properties']['base']['id'])
        counter += 1

    for _ in range(num_b):
        doc = Document('demoB')
        doc_struct = {'document_properties': doc.document_properties}

        doc_struct['document_properties']['demoB']['value'] = modify_value(value_modifier, counter)
        doc_struct['document_properties']['base']['id'] = modify_id(id_modifier, doc.id())
        doc_struct = modify_other_fields(other_modifier, doc_struct)
        doc_struct = remove_field(remover, doc_struct)

        docs.append(Document(doc_struct['document_properties']))
        node_names.append(str(counter))
        if 'id' in doc_struct['document_properties'].get('base', {}):
            ids_b.append(doc_struct['document_properties']['base']['id'])
        counter += 1

    c_count = 0
    for _ in range(num_c):
        doc = Document('demoC')
        doc_struct = {'document_properties': doc.document_properties}

        doc_struct['document_properties']['demoC']['value'] = modify_value(value_modifier, counter)
        doc_struct['document_properties']['base']['id'] = modify_id(id_modifier, doc.id())
        doc_struct = modify_other_fields(other_modifier, doc_struct)

        # Dependencies are set before modification
        new_doc = Document(doc_struct['document_properties'])
        dep_a = np.random.randint(0, num_a + 1)
        dep_b = np.random.randint(0, num_b + 1)
        dep_c = np.random.randint(0, c_count + 1)
        if dep_a > 0 and ids_a: new_doc.set_dependency_value('item1', ids_a[dep_a - 1])
        if dep_b > 0 and ids_b: new_doc.set_dependency_value('item2', ids_b[dep_b - 1])
        if dep_c > 0 and ids_c: new_doc.set_dependency_value('item3', ids_c[dep_c - 1])

        doc_struct = {'document_properties': new_doc.document_properties}
        doc_struct = modify_dependency(dependency_modifier, doc_struct)
        doc_struct = remove_field(remover, doc_struct)

        docs.append(Document(doc_struct['document_properties']))
        node_names.append(str(counter))
        if 'id' in doc_struct['document_properties'].get('base', {}):
            ids_c.append(doc_struct['document_properties']['base']['id'])
        counter += 1
        c_count += 1

    return None, node_names, docs

def verify_db_document_structure(db, G, expected_docs):
    """
    Verifies that a database contains the documents expected.
    """
    for doc in expected_docs:
        doc_id = doc.id()
        retrieved_doc = db.get_docs(doc_id)

        if not retrieved_doc:
            return False, f"Document with id {doc_id} not found in database."

        # This is a simplified comparison. A more robust implementation
        # would compare all fields.
        if doc.document_properties.get('demoA', {}).get('value') != retrieved_doc[0].document_properties.get('demoA', {}).get('value'):
            return False, f"Document {doc_id} value mismatch."
        if doc.document_properties.get('demoB', {}).get('value') != retrieved_doc[0].document_properties.get('demoB', {}).get('value'):
            return False, f"Document {doc_id} value mismatch."
        if doc.document_properties.get('demoC', {}).get('value') != retrieved_doc[0].document_properties.get('demoC', {}).get('value'):
            return False, f"Document {doc_id} value mismatch."

    return True, ""

def make_doc_tree(rates):
    """
    Makes a "tree" of documents to add to a database.
    """
    num_a = np.random.poisson(rates[0])
    num_b = np.random.poisson(rates[1])
    num_c = np.random.poisson(rates[2])

    docs = []
    node_names = []
    ids_a = []
    ids_b = []
    ids_c = []

    counter = 1

    for _ in range(num_a):
        doc = Document('demoA', **{'demoA.value': counter})
        docs.append(doc)
        node_names.append(str(counter))
        ids_a.append(doc.id())
        counter += 1

    for _ in range(num_b):
        doc = Document('demoB', **{'demoB.value': counter, 'demoA.value': counter})
        docs.append(doc)
        node_names.append(str(counter))
        ids_b.append(doc.id())
        counter += 1

    c_count = 0
    for _ in range(num_c):
        dep_a = np.random.randint(0, num_a + 1)
        dep_b = np.random.randint(0, num_b + 1)
        dep_c = np.random.randint(0, c_count + 1)

        doc = Document('demoC', **{'demoC.value': counter})

        if dep_a > 0:
            doc.set_dependency_value('item1', ids_a[dep_a - 1])
        if dep_b > 0:
            doc.set_dependency_value('item2', ids_b[dep_b - 1])
        if dep_c > 0:
            doc.set_dependency_value('item3', ids_c[dep_c - 1])

        docs.append(doc)
        node_names.append(str(counter))
        ids_c.append(doc.id())
        counter += 1
        c_count += 1

    return None, node_names, docs # G is not used in the python version
