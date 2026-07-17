\# Day 8 - PySpark File Formats



\## Topics Covered



\- Reading JSON files

\- Defining a manual schema

\- Writing CSV files

\- Reading and writing Parquet files

\- Comparing CSV and Parquet

\- Spark partitions

\- Repartitioning

\- Partitioned Parquet files



\## Files



\- `file\_formats.py` - PySpark program

\- `data/employees.json` - Sample employee data



\## Windows Note



JSON reading and schema operations run successfully.



Writing CSV or Parquet files on native Windows may require Hadoop Windows

utilities and the `HADOOP\_HOME` environment variable.



The same PySpark code can run in:



\- Databricks

\- Linux

\- WSL

\- A properly configured Spark environment

