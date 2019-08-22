import pandas as pd


def format_time(seconds):
    if seconds < 0.8:
        return "{:.0f}ms".format(seconds * 1000)
    if seconds < 60:
        return "{:.1f}s".format(seconds)
    if seconds < 3600:
        return "{:.1f}m".format(seconds / 60)
    return "{:.1f}h".format(seconds / 3600)


def unpack_frame(frame, unpack_column="config"):
    new = pd.DataFrame(list(frame[unpack_column]))
    new = pd.concat([frame, new], axis=1)
    new.drop(unpack_column, inplace=True, axis=1)
    return new