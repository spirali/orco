# Extensions

This chapter describes extensions to some external libraries.
These libraries are not installed with ORCO and extension
will throw an error if you used them without particular library.

  - [Matplotlib](#matplotlib)
  - [Pandas](#pandas)


## Matplotlib

Matplotlib extension allows to directly export charts as blobs.

```python
import orco
from orco.ext.pyplot import attach_figure
import matplotlib.pyplot as plt

@orco.builder()
def make_chart(name, values):

  plt.plot(values)
  plt.ylabel(name)

  # Instead of: plt.show()
  attach_plot("chart")
```

## Pandas

A builder can be easily exported into a Pandas `DataFrame`:

```python
from orco.ext.pandas import export_builder_to_pandas

# Exporting builder with name "builder1"
df = export_builder_to_pandas(runtime, "builder1")
```
