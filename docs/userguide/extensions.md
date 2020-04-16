# Extensions

This chapter describes extensions to some external libraries.
These libraries are not installed with ORCO and extension
will throw an error if you used them without particular library. 

  - [Matplotlib](#matplotlib)
  - [Pandas](#pandas)


## Matplotlib

TODO

## Pandas

A builder can be easily exported into a Pandas `DataFrame`:

```python
from orco.ext.pandas import export_builder_to_pandas

# Exporting builder with name "builder1"
df = export_builder_to_pandas(runtime, "builder1")
```
