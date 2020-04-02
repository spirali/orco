import orco


def attach_figure(name, format="png"):
    import matplotlib.pyplot as plt
    import io
    buf = io.BytesIO()
    plt.savefig(buf, format=format)
    buf.seek(0)
    if format == "png":
        mime = "image/png"
    else:
        mime = None
    orco.attach_bytes(name, buf.read(), mime)