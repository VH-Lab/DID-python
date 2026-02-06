import pandas as pd

def struct_name_value_search(the_struct, the_name, make_error=True):
    """
    Searches a list of dictionaries with 'name' and 'value' keys.

    This function mimics the behavior of the Matlab `struct_name_value_search` function.
    """
    if not isinstance(the_struct, list):
        raise TypeError("the_struct must be a list of dictionaries.")

    for i, item in enumerate(the_struct):
        if not isinstance(item, dict):
            raise TypeError("the_struct must be a list of dictionaries.")
        if 'name' not in item or 'value' not in item:
            raise ValueError("Each dictionary in the_struct must have 'name' and 'value' keys.")

        if item['name'] == the_name:
            return item['value'], i

    if make_error:
        raise ValueError(f"No matching entries for {the_name} were found.")
    else:
        return None, None

def table_cross_join(table1, table2, rename_conflicting_columns=False):
    """
    Performs a Cartesian product (SQL-style CROSS JOIN) of two pandas DataFrames.

    This function mimics the behavior of the Matlab `tableCrossJoin` function.
    """
    if not isinstance(table1, pd.DataFrame) or not isinstance(table2, pd.DataFrame):
        raise TypeError("Inputs must be pandas DataFrames.")

    conflicting_names = set(table1.columns) & set(table2.columns)

    if conflicting_names and not rename_conflicting_columns:
        raise ValueError(f"Input DataFrames have conflicting column names: {', '.join(conflicting_names)}. "
                         "Set 'rename_conflicting_columns' to True to automatically rename them.")

    if rename_conflicting_columns:
        table2 = table2.rename(columns={col: f"{col}_1" if col in conflicting_names else col for col in table2.columns})

    table1['_cross_join_key'] = 1
    table2['_cross_join_key'] = 1

    result_table = pd.merge(table1, table2, on='_cross_join_key').drop('_cross_join_key', axis=1)

    return result_table