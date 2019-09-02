import pandas as pd


def export_builder_to_pandas(runtime, builder_name):
    """
    Export builder into pandas  DataFrame
    """
    data = [(entry.config, entry.value, entry.comp_time)
            for entry in runtime.db.get_all_entries(builder_name)]
    return pd.DataFrame(data, columns=["config", "value", "comp_time"])
