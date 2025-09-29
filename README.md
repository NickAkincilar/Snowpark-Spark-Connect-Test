## SPARK DATA-ENGINERING TEST NOTEBOOK USING TPCH DELTA TABLES & MERGE

This notebook is designed to test a basic Spark Data Engineering notebook in terms of compatibility & performance when using with Snowflake-Spark-Connect which is a direct replacement for a Spark Session with no code changes.

It is designed to use **Delta** tables within Spark & the TPCH Dataset as source tables. This will allow you to run this on any platform that supports Spark (directly or via Spark-Connect) and has a sample TPCH dataset such as **Snowflake**, **EMR**, **Fabric** or **Databricks**. (Note the data volumnes may be different in different platforms even when TPCH scales are the same)

Below are the processing steps it goes through:
1. **Read Source Tables:** Load TPCH tables (customer, orders, lineitem, part, supplier, partsupp, nation, region) and standardize column names.
2. **Transformations:** Enrich customers with nation/region; compute order revenues; add window metrics (latest order, revenue rank, 30‑day rolling revenue).
3. **Create Target Tables:** Write curated Delta tables dim_customer, fact_orders, agg_customer_revenue (overwrite if exist).
4. **Simulate a CDC payload:** Sample 10K records(configurable); assign ops (**I**nsert/**U**pdate/**D**elete); tweak keys/fields for inserts/updates.
5. **Apply CDC (MERGE):** Use a single MERGE INTO to apply deletes, updates, and inserts to fact_orders.
6. **Post-CDC Check:** Count rows in fact_orders to validate updates.
7. **Refresh Aggregates:** Recompute and overwrite agg_customer_revenue from updated fact_orders.


### Link to the notebook   

[TPCH_SPARK_DELTA_DATA_ENGINEERING_PIPELINE](https://github.com/NickAkincilar/Snowpark-Spark-Connect-Test/blob/main/TPCH_SPARK_DELTA_DATA_ENGINEERING_PIPELINE.ipynb)
