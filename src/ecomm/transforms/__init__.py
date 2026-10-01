"""Pure DataFrame transformations, one module per layer.

Nothing in this package reads or writes storage, touches a catalog or uses dbutils:
every function takes DataFrames and returns DataFrames. That is what lets the same
code run on Databricks serverless (Spark Connect) and in local unit tests.

harness   Stream A replay + micro-batches, Stream B price-interval derivation.
"""
