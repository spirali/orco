import pandas as pd


def export_builder_to_pandas(runtime, builder_name, missing=pd.NA, arg_prefix="arg."):
    """
    Export builder into pandas  DataFrame
    """
    cols = {"value": [], "comp_time": []}
    for i, entry in enumerate(runtime.db.get_all_entries(builder_name)):
        cols["value"].append(entry.value)
        cols["comp_time"].append(entry.comp_time)
        for k, v in entry.config.items():
            n = arg_prefix + k
            if n not in cols:
                cols[n] = [missing] * i
            c = cols[n]
            if len(c) != i:
                c.extend([missing] * (i - len(c)))
            c.append(v)
    n = len(cols["value"])
    for c in cols.values():
        if len(c) != n:
            c.extend([missing] * (n - len(c)))
    return pd.DataFrame(cols)
