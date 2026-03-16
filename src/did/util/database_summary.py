"""Produce a summary dict of a DID database and its branches.

Mirrors MATLAB's did.util.databaseSummary for cross-language symmetry testing.
"""


def database_summary(db):
    """Return a dict summarizing every branch and document in *db*.

    The returned dict contains:
      - ``branchNames``: sorted list of all branch IDs
      - ``branchHierarchy``: dict mapping each branch name to its parent
      - ``branches``: dict keyed by branch name, each containing:
          - ``branchName``
          - ``docCount``
          - ``documents``: list of dicts with ``id``, ``className``, ``properties``
      - ``dbFilename``: empty string (caller should fill in)

    Documents within each branch are sorted by ID for determinism.
    """
    summary = {}
    summary["dbFilename"] = ""

    branch_names = db.all_branch_ids()
    if isinstance(branch_names, str):
        branch_names = [branch_names]
    branch_names = sorted(branch_names)
    summary["branchNames"] = branch_names

    # Branch hierarchy
    branch_hierarchy = {}
    for branch_name in branch_names:
        parent = db.get_branch_parent(branch_name)
        if parent is None:
            parent = ""
        branch_hierarchy[branch_name] = {
            "branchName": branch_name,
            "parent": parent,
        }
    summary["branchHierarchy"] = branch_hierarchy

    # Per-branch document summaries
    branches = {}
    for branch_name in branch_names:
        doc_ids = db.get_doc_ids(branch_name)
        if isinstance(doc_ids, str):
            doc_ids = [doc_ids]
        if not doc_ids:
            doc_ids = []
        doc_ids = sorted(doc_ids)

        doc_summaries = []
        for doc_id in doc_ids:
            doc = db.get_docs(doc_id)
            props = doc.document_properties

            class_name = ""
            dc = props.get("document_class", {})
            if isinstance(dc, dict):
                class_name = dc.get("class_name", "")

            doc_summaries.append(
                {
                    "id": doc_id,
                    "className": class_name,
                    "properties": props,
                }
            )

        branches[branch_name] = {
            "branchName": branch_name,
            "docCount": len(doc_ids),
            "documents": doc_summaries,
        }
    summary["branches"] = branches

    return summary
