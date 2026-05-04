<h1>Query Optimisation - Data Skipping & Data Pruning</h1>

---

**Contents**:

- [Data Skipping](#data-skipping)
- [Data Pruning](#data-pruning)

---

# Data Skipping

Data skipping is a performance optimisation technique in Delta Lake and Apache Spark that automatically collects file-level statistics (e.g. min/max values for columns, null counts, etc.) during data writes. By analysing this metadata, queries skip irrelevant files, reducing I/O and accelerating read performance. This is highly effective for filtering data, particularly when combined with techniques like Z-ordering.

\[NEEDS EXPANSION\]

# Data Pruning

Data pruning is a performance optimisation technique that reduces the amount of data scanned and processed by queries, which leads to faster execution and lower costs. Databricks employs several automatic and manual pruning methods.

\[NEEDS EXPANSION\]