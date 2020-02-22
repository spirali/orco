import threading

# TODO: In future, change to ContextVar
_CONTEXT = threading.local()
_CONTEXT.on_entry = None
