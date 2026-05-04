<h1>Auto Loader</h1>

---

**Contents**:

- [Concept](#concept)
- [Auto Loader as a Cloud File Ingestion Tool](#auto-loader-as-a-cloud-file-ingestion-tool)
  - [Core Functionality](#core-functionality)
  - [cloudFiles](#cloudfiles)
  - [In Databricks](#in-databricks)
  - [Advanced Usage](#advanced-usage)

---

# Concept

Auto Loader is an **optimised cloud file source** for Apache Spark that loads data continuously and efficiently from cloud storage as new data arrives. “Continuously” means it automatically and periodically loads new-arriving data, whereas “efficiently” means it maintains a level of performance (low latency, high throughput) in data loading that approximates real-time ingestion.

Being a cloud file source means it can be treated as yet another data source by Spark applications, thereby abstracting the particular loading mechanisms and optimisations that give rise to its functionality. But, if we think of its functionality, we see that it is essentially a **data ingestion tool** for cloud files.

Hence, 2 conceptualisations of Auto Loader:

1. Cloud file source (from Spark app’s perspective)  
2. Cloud file data ingestion tool (from data engineer’s perspective)

> **References**:
> 
> - [Simplifying Data Ingestion with Auto Loader for Delta Lake](https://www.databricks.com/blog/2020/02/24/introducing-databricks-ingest-easy-data-ingestion-into-delta-lake.html)
> - [Streaming Data Ingestion with Databricks Auto Loader by Shubhodaya Hampiholi on Medium](https://medium.com/@shubhodaya.hampiholi/streaming-data-ingestion-with-databricks-auto-loader-8216d4ad3b67)
> - [What is Auto Loader? | Databricks on AWS](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)

For data engineering purposes, we shall consider it as a cloud file data ingestion tool.

# Auto Loader as a Cloud File Ingestion Tool

> **Main reference**: [What is Auto Loader? | Databricks on AWS](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)

## Core Functionality

An Auto Loader is a tool to incrementally (in recurring time periods) and efficiently (with low latency) process new data files as they arrive in a cloud storage, without any additional setup required to do this processing. An Auto Loader provides a structured streaming source: **cloudFiles** (*since, as data engineers, we are not abstracting away the functionality, we can further differentiate the components of an Auto Loader into a data structure (i.e. the “structured streaming source”, into which data is loaded) and processes*).

## cloudFiles

Given an input directory path on the cloud file storage from where data is being ingested, the cloudFiles source automatically processes new files as they arrive, with the option of also processing existing files in the given input directory path.

## In Databricks

- **Auto Loader is a part of Lakeflow Spark Declarative Pipelines**
- Has support for both SQL and Python

## Advanced Usage

You can use Auto Loader to process billions of files to migrate or backfill a table. Auto Loader also scales automatically to support near-real-time ingestion of millions of files per hour.