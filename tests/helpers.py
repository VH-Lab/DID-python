import numpy as np
from scipy.sparse import lil_matrix
from did.document import Document

def make_doc_tree(rates):
    """
    Makes a "tree" of documents to add to a database.
    """
    num_a = np.random.poisson(rates[0])
    num_b = np.random.poisson(rates[1])
    num_c = np.random.poisson(rates[2])

    g = lil_matrix((num_a + num_b + num_c, num_a + num_b + num_c))

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
        node_names.append(str(counter))
        ids_c.append(doc.id())

        if dep_a > 0:
            doc.add_dependency('item1', ids_a[dep_a - 1])
            g[dep_a - 1, counter - 1] = 1

        if dep_b > 0:
            doc.add_dependency('item2', ids_b[dep_b - 1])
            g[num_a + dep_b - 1, counter - 1] = 1

        if dep_c > 0:
            doc.add_dependency('item3', ids_c[dep_c - 1])
            g[num_a + num_b + dep_c - 1, counter - 1] = 1

        docs.append(doc)
        counter += 1
        c_count += 1

    return g, node_names, docs

def verify_db_document_structure(db, g, expected_docs):
    """
    Verifies that a database contains the documents expected.
    """
    fieldset = ['demoA', 'demoB', 'demoC']

    for i, expected_doc in enumerate(expected_docs):
        id_here = expected_doc.id()
        doc_here = db.get_docs(id_here)

        if not doc_here:
            return False, f"Document with id {id_here} not found in the database."

        doc_here = doc_here[0] # get_docs returns a list

        for field in fieldset:
            if f"{field}.value" in expected_doc.document_properties:
                if f"{field}.value" not in doc_here.document_properties:
                    return False, f"Field '{field}' not found in document {id_here}."

                expected_value = expected_doc.document_properties[f"{field}.value"]
                actual_value = doc_here.document_properties[f"{field}.value"]
                if expected_value != actual_value:
                    return False, f"Field '{field}' of document {id_here} did not match."

    return True, ""
