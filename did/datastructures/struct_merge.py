def struct_merge(s1, s2, error_if_new_field=False, do_alphabetical=True):
    """
    Merges two dictionaries into a common dictionary.

    Args:
        s1 (dict): The first dictionary.
        s2 (dict): The second dictionary.
        error_if_new_field (bool): If True, raise an error if s2 contains a
            field that is not present in s1.
        do_alphabetical (bool): If True, alphabetize the field names in the
            result.

    Returns:
        dict: The merged dictionary.
    """

    if error_if_new_field:
        missing_field_names = set(s2.keys()) - set(s1.keys())
        if missing_field_names:
            raise ValueError(f"Some fields of the second dictionary are not in the first: {missing_field_names}")

    s_out = s1.copy()
    s_out.update(s2)

    if do_alphabetical:
        return {key: s_out[key] for key in sorted(s_out)}

    return s_out
