import pandas as pd


def export_builder(runtime, builder_name, missing=pd.NA, arg_prefix="arg."):
    """
    Export builder into pandas  DataFrame
    """

    cols = {"comp_time": []}
    for i, job in enumerate(runtime.db.export_builder(builder_name)):
        cols["comp_time"].append(job.computation_time)
        for k, v in job.config.items():
            n = arg_prefix + k
            if n not in cols:
                cols[n] = [missing] * i
            c = cols[n]
            if len(c) != i:
                c.extend([missing] * (i - len(c)))
            c.append(v)
    return pd.DataFrame(cols)


def unpack_frame(frame, unpack_column="config"):
    new = pd.DataFrame(list(frame[unpack_column]))
    new = pd.concat([frame, new], axis=1)
    new.drop(unpack_column, inplace=True, axis=1)
    return new
