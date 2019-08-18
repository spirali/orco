import pandas as pd


def export_collection_to_pandas(runtime, collection_name):
    data = [(entry.config, entry.value, entry.comp_time)
            for entry in runtime.db.get_all_entries(collection_name)]
    return pd.DataFrame(data, columns=["config", "value", "comp_time"])
