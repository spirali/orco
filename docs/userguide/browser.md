# ORCO browser

ORCO contains a web browser of computations and their results stored in the database.
It can be started by running:

```python
runtime.serve()
```

> The `serve` method is blocking. If you want to start the browser and run computations in the same script,
> you can call `serve(nonblocking=True)` before calling `compute` on the `Runtime`.

`serve` starts a local HTTP server (by default on port 8550) that allows inspecting
stored data in the database and observing tasks running in executors. It is completely safe to
run computation(s) simultaneously with the server.

![Screenshot of ORCO browser](./imgs/browser-collection.png)
