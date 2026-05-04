# Databricks for Data Engineering

**Main references**:

* [Databricks Certified Data Engineer Associate \- Preparation | Udemy](https://www.udemy.com/course/databricks-certified-data-engineer-associate/)  
* [Data Intelligence using Databricks](https://docs.google.com/document/d/1-ZXqy_fFBAb_8EMDXSUfP5eEdwEqYoKOMStMPGwo0ZA/)

**Table of Contents**

[**Course Outline	3**](#course-outline)

[**Databricks Workspace	3**](#databricks-workspace)

[**Delta Lake & Delta Tables	4**](#delta-lake-&-delta-tables)

[Immutability of Data Files	4](#immutability-of-data-files)

[Transaction Log (Delta Log): The Keystone of Delta Lake	5](#transaction-log-\(delta-log\):-the-keystone-of-delta-lake)

[Essential Utility	5](#essential-utility)

[Important Clarifications	6](#important-clarifications)

[Query Engine Relies on Transaction Logs	6](#query-engine-relies-on-transaction-logs)

[Checksum Files for Transaction Logs	7](#checksum-files-for-transaction-logs)

[Updating a Delta Table Using UPDATE	7](#updating-a-delta-table-using-update)

[**Advanced Features of Delta Tables	8**](#advanced-features-of-delta-tables)

[Time Travel	8](#time-travel)

[Audit Data Changes	9](#audit-data-changes)

[Query Older Versions of Data	9](#query-older-versions-of-data)

[Rollback Versions	10](#rollback-versions)

[Compaction (Small File Problem)	10](#compaction-\(small-file-problem\))

[Vacuum (Cleaning Up Unused Files)	11](#vacuum-\(cleaning-up-unused-files\))

[**Data File Layout Optimization for Delta Tables	13**](#data-file-layout-optimization-for-delta-tables)

[What Is It & What Is It For?	13](#what-is-it-&-what-is-it-for?)

[Layout Optimization Techniques	13](#layout-optimization-techniques)

[Partitioning (Hive-style Partitioning)	13](#partitioning-\(hive-style-partitioning\))

[Z-Order Indexing	15](#z-order-indexing)

[Liquid Clustering	16](#liquid-clustering)

[Choosing the Right Optimization Technique	18](#choosing-the-right-optimization-technique)

# Course Outline {#course-outline}

1. Databricks lakehouse platform  
2. ETL (extract-transform-load) with Spark, SQL and Python  
3. Incremental data processing  
4. Production pipelines  
5. Data governance

# Databricks Workspace {#databricks-workspace}

**Conceptual Explanation:**

Databricks Workspace is your collaborative cloud environment for data engineering, data science, and analytics. Think of it as an IDE (Integrated Development Environment) specifically designed for big data workloads, where you can write code, manage data, and collaborate with your team.

**Practical Guidelines:**

**Git Folders (formerly "Repos"):**

- Git folders allow you to connect your Databricks workspace directly to Git repositories (GitHub, GitLab, Azure DevOps, etc.)  
- This enables version control for your notebooks and code files  
- **Where to start:**  
  - Navigate to the Workspace menu in Databricks  
  - Look for "Repos" or "Git folders" option  
  - Click "Add Repo" and connect to your Git repository  
  - You can now commit, push, and pull changes directly from Databricks

**Notebook Features:**

- Databricks notebooks support multiple languages: Python, SQL, Scala, and R (can mix in single notebook using magic commands like `%python`, `%sql`)  
- Built-in visualization tools for data exploration  
- Collaborative editing with real-time commenting  
- **Where to start:**  
  - Create a new notebook from Workspace → Create → Notebook  
  - Use `Shift+Enter` to run cells  
  - Explore the "Data" tab to browse available tables  
  - Use `display()` function for interactive visualizations

**References:**

- [Databricks Workspace Documentation](https://docs.databricks.com/workspace/index.html)  
- [Git Integration with Databricks](https://docs.databricks.com/repos/index.html)

---

# Delta Lake & Delta Tables {#delta-lake-&-delta-tables}

**Conceptual Explanation:**

Delta Lake is an open-source storage framework that brings reliability and performance to data lakes. It's built on top of Parquet files and adds a transaction log layer that provides ACID (Atomicity, Consistency, Isolation, Durability) guarantees—features typically found only in traditional databases.

Think of Delta Lake as a reliability layer that sits between your raw data storage and your query engines, ensuring data quality and consistency.

## Immutability of Data Files {#immutability-of-data-files}

**Conceptual Explanation:**

Immutability means that once a Parquet file is written, it cannot be modified. This isn't just a technical limitation—it's a design choice that provides several benefits:

- **Version control:** Each file represents a snapshot of data at a point in time  
- **Reliability:** No risk of corrupting existing data during updates  
- **Concurrency:** Multiple readers can access files without conflicts

When you "update" data in Delta Lake, you're actually:

1. Creating new Parquet files with the updated data  
2. Marking old files as inactive in the transaction log  
3. The old files remain on storage (until explicitly cleaned up)

**Practical Guidelines:**

- When you run an `UPDATE`, `DELETE`, or `MERGE` operation, new files are created behind the scenes  
- Old files are retained by default, enabling time travel (querying historical data)  
- Use `VACUUM` command periodically to clean up old files (more on this below)

**References:**

- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)  
- [Apache Parquet Documentation](https://parquet.apache.org/docs/)

## Transaction Log (Delta Log): The Keystone of Delta Lake {#transaction-log-(delta-log):-the-keystone-of-delta-lake}

**Conceptual Explanation:**

The transaction log (stored in the `_delta_log/` subfolder) is the "single source of truth" for a Delta table. It's a JSON-based record of every transaction that has ever occurred on the table.

Think of it like a ledger in accounting: every change is recorded chronologically, creating a complete audit trail. This log tells the query engine:

- Which data files are currently valid  
- Which files have been removed or superseded  
- The schema of the table at any point in time  
- Statistics about the data (for query optimization)

## Essential Utility {#essential-utility}

**How Transaction Logs Ensure Data Reliability:**

**1\. Preventing Dirty Reads:**

- When writing data, the file is written first, then added to the transaction log atomically  
- If the write fails halfway, the file is never referenced in the log  
- Readers only see complete, committed transactions

**2\. Ensuring Fresh Data:**

- The log explicitly tracks which version of each file is current  
- Old versions are marked as removed but not physically deleted immediately  
- Query engines read the log to determine the current table state

**3\. Avoiding Deadlocks:**

- Multiple readers can read simultaneously without blocking  
- Writers use optimistic concurrency control (more advanced topic)  
- The log coordinates access without traditional locking mechanisms

**Together, these enable:**

- **ACID transactions on object storage:** Cloud storage (S3, ADLS, GCS) isn't transactional by default; Delta Lake adds this capability  
- **Scalable metadata handling:** Metadata is distributed across log files, not in a single bottleneck  
- **Complete audit trail:** Every change is recorded and can be traced  
- **Standard format compatibility:** Uses Parquet for data and JSON for logs—both open standards

**Practical Guidelines:**

- Transaction logs are automatically managed—you typically don't interact with them directly  
- Each commit creates a new JSON file in `_delta_log/` (numbered sequentially: 00000.json, 00001.json, etc.)  
- **Where to look:** Use `DESCRIBE HISTORY` command to view transaction history in human-readable format

## Important Clarifications {#important-clarifications}

### Query Engine Relies on Transaction Logs {#query-engine-relies-on-transaction-logs}

**Conceptual Explanation:**

When you query a Delta table, the query engine:

1. Reads the latest transaction log entry  
2. Determines which Parquet files are valid for the current version  
3. Ignores all other files in the directory  
4. Executes the query only against valid files

This is why you can have "deleted" files still present in storage—they're simply not referenced in the current log state.

### Checksum Files for Transaction Logs {#checksum-files-for-transaction-logs}

**Conceptual Explanation:**

The `_delta_log/` folder contains:

- **Transaction log files** (JSON): Record each transaction  
- **Checkpoint files** (Parquet): Periodic snapshots of the log state for faster reads  
- **CRC (Cyclic Redundancy Check) files**: Checksums to verify log file integrity

Checksum files ensure that transaction logs haven't been corrupted during storage or transfer. This is crucial for maintaining data reliability.

**Practical Guidelines:**

- Every 10 commits, Delta Lake automatically creates a checkpoint file (configurable)  
- Checkpoints allow faster "catch-up" when reading the table state  
- You generally don't need to manage these manually

### Updating a Delta Table Using UPDATE {#updating-a-delta-table-using-update}

**Conceptual Explanation:**

When you run an `UPDATE` statement:

1. Delta Lake identifies which files contain rows matching the WHERE clause  
2. Reads those files  
3. Creates new files with the updated values  
4. Adds the new files to the transaction log  
5. Marks the old files as removed in the transaction log

The old files remain on storage (for time travel purposes) until explicitly cleaned with `VACUUM`.

**Practical Guidelines:**

\-- Example UPDATE operation

UPDATE my\_table

SET column1 \= 'new\_value'

WHERE column2 \= 'condition';

\-- Behind the scenes:

\-- \- New Parquet files created with updated rows

\-- \- Old Parquet files marked as removed in transaction log

\-- \- Query engine now reads only the new files

**References:**

- [Delta Lake Transaction Log Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)  
- [Understanding Delta Lake's Transaction Log](https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html)

---

# Advanced Features of Delta Tables {#advanced-features-of-delta-tables}

## Time Travel {#time-travel}

**Conceptual Explanation:**

Time Travel allows you to query historical versions of your data. Because Delta Lake retains old files and records all changes in the transaction log, you can access data as it existed at any previous point in time.

Use cases:

- Auditing: "What did this record look like last month?"  
- Rollback: "Undo that accidental DELETE"  
- Reproducibility: "Use the exact data version from the experiment"  
- Comparison: "How has this data changed over time?"

### Audit Data Changes {#audit-data-changes}

**Practical Guidelines:**

\-- View full history of a table

DESCRIBE HISTORY my\_table;

\-- Output includes:

\-- \- Version number

\-- \- Timestamp

\-- \- Operation (CREATE, UPDATE, DELETE, MERGE, etc.)

\-- \- User who performed the operation

\-- \- Number of rows affected

**Conceptual Note:** This gives you a complete audit trail of all changes made to the table.

### Query Older Versions of Data {#query-older-versions-of-data}

**Practical Guidelines:**

**Three ways to time travel:**

\-- 1\. By version number

SELECT \* FROM my\_table VERSION AS OF 5;

\-- 2\. By timestamp

SELECT \* FROM my\_table TIMESTAMP AS OF '2024-01-15T10:30:00';

\-- 3\. Using @ syntax (version only)

SELECT \* FROM my\_table@v5;

**Where to start:**

- First, run `DESCRIBE HISTORY` to see available versions  
- Choose the version or timestamp you need  
- Query using one of the syntax options above

### Rollback Versions {#rollback-versions}

**Conceptual Explanation:**

If you made a mistake (e.g., accidentally deleted data), you can restore the table to a previous version. This creates a new version in the transaction log that matches the old state.

**Practical Guidelines:**

\-- Restore to a specific version

RESTORE TABLE my\_table TO VERSION AS OF 5;

\-- Restore to a specific timestamp

RESTORE TABLE my\_table TO TIMESTAMP AS OF '2024-01-15T10:30:00';

**Important:** RESTORE creates a new version; it doesn't delete the "bad" versions. Your audit trail remains intact.

**References:**

- [Delta Lake Time Travel](https://docs.delta.io/latest/delta-batch.html#deltatimetravel)

## Compaction (Small File Problem) {#compaction-(small-file-problem)}

**Conceptual Explanation:**

Over time, Delta tables can accumulate many small files, especially with:

- Streaming ingestion (many micro-batches)  
- Frequent updates/deletes  
- Unoptimized write patterns

Having many small files hurts query performance because:

- More files \= more metadata to process  
- Small files \= poor I/O efficiency  
- Overhead of opening/closing many files

**Compaction** combines small files into larger, optimally-sized files.

**Practical Guidelines:**

\-- Basic compaction

OPTIMIZE my\_table;

\-- With Z-Ordering (explained below)

OPTIMIZE my\_table ZORDER BY (column1, column2);

**When to use:**

- Run `OPTIMIZE` periodically (daily, weekly, depending on write frequency)  
- After large batch operations  
- When query performance degrades

**Conceptual Note:** OPTIMIZE is a read-heavy, write-heavy operation. Schedule during low-traffic periods if possible.

**References:**

- [Databricks OPTIMIZE Command](https://docs.databricks.com/sql/language-manual/delta-optimize.html)

## Vacuum (Cleaning Up Unused Files) {#vacuum-(cleaning-up-unused-files)}

**Conceptual Explanation:**

`VACUUM` physically deletes files that are no longer referenced in the transaction log. This includes:

- Old versions of updated/deleted data  
- Files from failed transactions  
- Files older than the retention threshold

**Why not delete immediately?**

- Time travel requires old files  
- Running queries might still be reading old versions  
- Safety buffer to prevent accidental data loss

**Practical Guidelines:**

\-- Default: Delete files older than 7 days (default retention)

VACUUM my\_table;

\-- Specify custom retention period

VACUUM my\_table RETAIN 168 HOURS; \-- 7 days in hours

\-- To vacuum aggressively (CAUTION: disables safety check)

SET spark.databricks.delta.retentionDurationCheck.enabled \= false;

VACUUM my\_table RETAIN 0 HOURS;

**Important Safety Notes:**

- **Default retention:** 7 days (168 hours)  
- You cannot time travel beyond the retention period after vacuuming  
- Disabling the retention check can cause active queries to fail if they're reading old versions  
- Be cautious when setting aggressive retention periods

**When to use:**

- Periodically (weekly/monthly) to control storage costs  
- After major data operations when you're sure you don't need old versions  
- When storage space is a concern

**References:**

- [Delta Lake VACUUM](https://docs.delta.io/latest/delta-utility.html#vacuum)  
- [Databricks VACUUM Command](https://docs.databricks.com/sql/language-manual/delta-vacuum.html)

---

# Data File Layout Optimization for Delta Tables {#data-file-layout-optimization-for-delta-tables}

## What Is It & What Is It For? {#what-is-it-&-what-is-it-for?}

**Conceptual Explanation:**

**Data file layout** refers to how data is physically organized and stored in files. Different organization strategies can dramatically improve query performance.

**Data skipping** is the key optimization technique enabled by good layout:

- When querying, the engine reads metadata about each file  
- If the metadata shows a file can't possibly contain matching data, it's skipped entirely  
- Fewer files read \= faster queries

Example: If you're querying `WHERE date = '2024-01-15'` and a file's metadata shows it only contains dates from 2024-02-01 onwards, that file is skipped.

**Goal:** Organize data so that queries can skip as many irrelevant files as possible.

## Layout Optimization Techniques {#layout-optimization-techniques}

### Partitioning (Hive-style Partitioning) {#partitioning-(hive-style-partitioning)}

**Conceptual Explanation:**

Partitioning physically separates data into different directories based on column values. Think of it like organizing files in folders by year, then month, then day.

**How it works:**

- Data is split into separate subdirectories based on partition column(s)  
- Each subdirectory contains only files with that partition value  
- Queries filtering on partition columns can skip entire directories

**Example structure:**

my\_table/

  year=2024/

    month=01/

    part-001.parquet

    part-002.parquet

    month=02/

    part-003.parquet

  year=2023/

    ...

**When to use:**

- **High cardinality columns used frequently in filters** (e.g., date, region, customer\_id)  
- Columns with relatively few distinct values (ideally hundreds, not millions)  
- Columns that rarely change (updates across partitions are expensive)

**When NOT to use:**

- High cardinality columns (millions of distinct values) \= too many directories  
- Columns used rarely in queries  
- Columns that change frequently

**Practical Guidelines:**

\-- Create partitioned table

CREATE TABLE my\_table (

  id INT,

  name STRING,

  date DATE

) USING DELTA

PARTITIONED BY (date);

\-- Query leveraging partitioning

SELECT \* FROM my\_table WHERE date \= '2024-01-15';

\-- Only reads files in the date=2024-01-15 directory

**Best practices:**

- Common partition columns: date, country, region, category  
- Aim for partitions with at least 1GB of data each  
- Avoid over-partitioning (thousands of tiny partitions)

**References:**

- [Delta Lake Partitioning](https://docs.delta.io/latest/best-practices.html#partition-data)

### Z-Order Indexing {#z-order-indexing}

**Conceptual Explanation:**

Z-Ordering is a technique that **co-locates related data** in the same files by clustering data along multiple columns simultaneously.

Unlike partitioning (which uses separate directories), Z-Ordering works within files by:

- Sorting data using a space-filling curve algorithm (Z-order curve)  
- Clustering rows with similar values in specified columns together  
- Improving data skipping for multi-dimensional queries

**Analogy:** Imagine organizing books in a library. Partitioning is like having separate rooms for each genre. Z-Ordering is like arranging books within each room by author AND publication year simultaneously.

**When to use:**

- Queries filter on multiple columns (multi-dimensional filtering)  
- Columns have high cardinality (not suitable for partitioning)  
- You want data skipping without creating separate directories

**Practical Guidelines:**

\-- Apply Z-Ordering during OPTIMIZE

OPTIMIZE my\_table

ZORDER BY (customer\_id, transaction\_date);

\-- Query benefiting from Z-Ordering

SELECT \* FROM my\_table

WHERE customer\_id \= 12345

  AND transaction\_date BETWEEN '2024-01-01' AND '2024-01-31';

\-- Files are organized so matching data is clustered, enabling better skipping

**Best practices:**

- Z-Order on columns frequently used together in WHERE clauses  
- Typically 2-4 columns (diminishing returns beyond that)  
- Run Z-Order periodically after significant data changes  
- Combine with partitioning for best results (partition by low-cardinality column, Z-Order by high-cardinality columns)

**References:**

- [Z-Ordering (Multi-Dimensional Clustering)](https://docs.databricks.com/delta/data-skipping.html#z-ordering-multi-dimensional-clustering)

### Liquid Clustering {#liquid-clustering}

**Conceptual Explanation:**

Liquid Clustering is a **newer, more flexible alternative** to both partitioning and Z-Ordering. It incrementally clusters data as it's written, without requiring manual optimization or creating rigid directory structures. In Databricks Runtime 15.4 LTS and above, you can enable automatic liquid clustering for Unity Catalog managed Delta tables. Automatic liquid clustering allows Databricks to intelligently choose clustering keys to optimize query performance based on your usage patterns, using the `CLUSTER BY AUTO` clause.

**NOTE**: *By contrast, Z-Ordering is not incremental, since it requires the reorganization and rewriting of the whole table’s files.*

**Key advantages:**

- No need to choose partition columns upfront (easier to adapt as query patterns change)  
- Handles high-cardinality columns better than partitioning  
- Automatically maintained with each write  
- Can change clustering columns without rewriting the entire table

**How it differs:**

- **Partitioning:** Rigid directory structure, hard to change  
- **Z-Ordering:** Requires periodic manual optimization  
- **Liquid Clustering:** Flexible, automatic, incremental

**When to use:**

- New tables where query patterns might evolve  
- High-cardinality columns unsuitable for partitioning  
- You want automatic optimization without manual OPTIMIZE commands  
- Workloads with frequent writes and diverse query patterns

**Practical Guidelines:**

\-- Create table with Liquid Clustering

CREATE TABLE my\_table (

  id INT,

  customer\_id INT,

  transaction\_date DATE,

  amount DECIMAL(10,2)

)

USING DELTA

CLUSTER BY (customer\_id, transaction\_date);

\-- Change clustering columns (flexible\!)

ALTER TABLE my\_table CLUSTER BY (transaction\_date, amount);

\-- Clustering happens automatically on writes \- no OPTIMIZE needed

**Best practices:**

- Choose 2-4 clustering columns based on most common query filters  
- Order matters: put most selective column first  
- Liquid Clustering is recommended for new tables in Databricks Runtime 13.0+  
- Consider migrating from partitioning/Z-Order for better flexibility

**Limitations:**

- Requires Databricks Runtime 13.0+ (or Delta Lake 3.0+)  
- Still evolving feature (check latest documentation for updates)

**References:**

- [Liquid Clustering](https://docs.databricks.com/delta/clustering.html)  
- [Liquid Clustering Announcement](https://www.databricks.com/blog/2023/04/24/introducing-liquid-clustering-delta-lake.html)

---

## Choosing the Right Optimization Technique {#choosing-the-right-optimization-technique}

**Practical Decision Guide:**

| Scenario | Recommended Approach |
| :---- | :---- |
| Date-based filtering (e.g., daily reports) | **Partition by date** |
| Multi-column filtering (e.g., customer \+ date) | **Partition by date \+ Z-Order by customer\_id** |
| High-cardinality columns (millions of values) | **Liquid Clustering** or **Z-Order** |
| Evolving query patterns | **Liquid Clustering** |
| Legacy table with fixed patterns | **Partitioning \+ Z-Order** |
| Write-heavy streaming workload | **Liquid Clustering** |

**General References:**

- [Delta Lake Best Practices](https://docs.delta.io/latest/best-practices.html)  
- [Databricks Performance Tuning Guide](https://docs.databricks.com/optimizations/index.html)  
- [Delta Lake on GitHub](https://github.com/delta-io/delta)