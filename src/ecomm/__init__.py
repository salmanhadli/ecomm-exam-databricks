"""ecomm — the library behind every notebook and pipeline in this bundle.

Notebooks stay thin: they read parameters, call functions from here, and record
evidence. All business logic lives in this package so it can be unit-tested locally
(tests/unit) and read in one place.

Modules
-------
settings    Defended thresholds and constants (noise threshold, windows, floors).
project     Unity Catalog naming: catalog, schemas, tables, volume paths.
history     Table-history retention policy and its guard (no VACUUM habit).
evidence    Recorder that appends every measurement to ops.measurements.
runtime     Notebook bootstrap: job parameters, session time zone, evidence.
schemas     Spark schemas shared across layers.
transforms  Pure DataFrame transformations, one module per layer.
"""
