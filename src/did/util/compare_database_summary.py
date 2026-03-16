"""Compare two database summaries and return a list of discrepancy messages.

Mirrors MATLAB's did.util.compareDatabaseSummary for cross-language symmetry testing.
"""


def compare_database_summary(summary_a, summary_b):
    """Compare two summary dicts and return a list of mismatch descriptions.

    Returns an empty list when the summaries are equivalent.
    """
    report = []

    branches_a = summary_a.get("branchNames", [])
    branches_b = summary_b.get("branchNames", [])

    only_in_a = set(branches_a) - set(branches_b)
    only_in_b = set(branches_b) - set(branches_a)

    for name in sorted(only_in_a):
        report.append(f'Branch "{name}" exists only in summary A.')
    for name in sorted(only_in_b):
        report.append(f'Branch "{name}" exists only in summary B.')

    # Compare branch hierarchy
    hier_a = summary_a.get("branchHierarchy", {})
    hier_b = summary_b.get("branchHierarchy", {})
    common_branches = sorted(set(branches_a) & set(branches_b))

    for branch_name in common_branches:
        if branch_name in hier_a and branch_name in hier_b:
            parent_a = hier_a[branch_name].get("parent", "")
            parent_b = hier_b[branch_name].get("parent", "")
            if parent_a != parent_b:
                report.append(
                    f'Branch "{branch_name}": parent mismatch '
                    f'("{parent_a}" vs "{parent_b}").'
                )

    # Compare per-branch documents
    br_a = summary_a.get("branches", {})
    br_b = summary_b.get("branches", {})

    for branch_name in common_branches:
        if branch_name not in br_a or branch_name not in br_b:
            continue

        branch_a = br_a[branch_name]
        branch_b = br_b[branch_name]

        if branch_a["docCount"] != branch_b["docCount"]:
            report.append(
                f'Branch "{branch_name}": doc count mismatch '
                f'({branch_a["docCount"]} vs {branch_b["docCount"]}).'
            )

        map_a = {d["id"]: d for d in branch_a.get("documents", [])}
        map_b = {d["id"]: d for d in branch_b.get("documents", [])}

        missing_in_a = sorted(set(map_b) - set(map_a))
        missing_in_b = sorted(set(map_a) - set(map_b))

        for doc_id in missing_in_a:
            report.append(
                f'Branch "{branch_name}": doc "{doc_id}" missing in summary A.'
            )
        for doc_id in missing_in_b:
            report.append(
                f'Branch "{branch_name}": doc "{doc_id}" missing in summary B.'
            )

        for doc_id in sorted(set(map_a) & set(map_b)):
            doc_a = map_a[doc_id]
            doc_b = map_b[doc_id]

            if doc_a.get("className", "") != doc_b.get("className", ""):
                report.append(
                    f'Branch "{branch_name}", doc "{doc_id}": class name mismatch '
                    f'("{doc_a["className"]}" vs "{doc_b["className"]}").'
                )

            props_a = doc_a.get("properties", {})
            props_b = doc_b.get("properties", {})

            for field in ("demoA", "demoB", "demoC"):
                has_a = field in props_a
                has_b = field in props_b
                if has_a and has_b:
                    if props_a[field] != props_b[field]:
                        report.append(
                            f'Branch "{branch_name}", doc "{doc_id}": '
                            f"{field} mismatch."
                        )
                elif has_a != has_b:
                    report.append(
                        f'Branch "{branch_name}", doc "{doc_id}": '
                        f'field "{field}" present in one summary but not the other.'
                    )

            # Compare depends_on
            deps_a = props_a.get("depends_on", [])
            deps_b = props_b.get("depends_on", [])
            if isinstance(deps_a, dict):
                deps_a = [deps_a]
            if isinstance(deps_b, dict):
                deps_b = [deps_b]
            norm_a = [
                (d.get("name", ""), d.get("value", ""))
                for d in deps_a
                if isinstance(d, dict)
            ]
            norm_b = [
                (d.get("name", ""), d.get("value", ""))
                for d in deps_b
                if isinstance(d, dict)
            ]
            if norm_a != norm_b:
                report.append(
                    f'Branch "{branch_name}", doc "{doc_id}": depends_on mismatch.'
                )

    return report
