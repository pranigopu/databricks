<h1>Layout Optimisation - Liquid Clustering</h1>

---

**Contents**:

- [Concept](#concept)
- [Practical Guidelines](#practical-guidelines)

---

# Concept

Liquid clustering is based on 2 key ideas:

- Clustering new data files first after writing them <br> *Here, the compaction should be done using OPTIMIZE later*  
- Keeping a track of data layout optimality and incrementally optimising it over time <br> *This ensures that the layout is optimised eventually rather than in one shot*

Liquid clustering uses a tree-based algorithm to optimise your data layout. The algorithm optimises for a balanced data layout, which means (1) uniform file size and (2) an appropriate number of files for the size of the dataset. This automatically takes care of data skew and the small file problem.

Based on this tree-based algorithm, older data which may be less well-clustered are incrementally clustered with every run of OPTIMIZE. To be more precise about this point, liquid clustering is stateful, i.e. it uses the metadata stored in the Delta Lake transaction logs to keep track of the data layout as your dataset evolves. This makes incremental clustering possible: any new data that you add to the dataset is clustered without having to recompute the layout for the existing data, since the state of the dataset as a whole is saved and can be referenced later for incremental optimisation. Because of this, liquid clustering is much more efficient than traditional partitioning and Z-ordering.

*Therefore*…

Liquid clustering provides flexibility to redefine clustering keys without rewriting existing data, allowing data layout to evolve alongside analytic needs over time. Liquid clustering applies to both Streaming Tables and Materialized Views. Delta Lake liquid clustering replaces table partitioning and ZORDER to simplify data layout decisions and optimise query performance.

> **References**:
> 
> - [Delta Lake Liquid Clustering](https://delta.io/blog/liquid-clustering/)
> - [Liquid Clustering for Delta Tables | by Vinay kumar | Medium](https://medium.com/@vinaykumar1393.vk/liquid-clustering-for-delta-tables-983b4b93b58b)

# Practical Guidelines

Liquid clustering works as below for write operations:

1. spark.write.format('delta').mode('append') <br> *Supported — batch appends honor cluster on write*  
1. spark.writeStream.format("delta").mode("append")  
   1. Data is appended incrementally in micro-batches  
   2. Clustering is not applied during the write operation  
   3. Clustering must be maintained through background optimization processes <br> (e.g. OPTIMIZE)

> **References**:
> 
> - My boss
> - [Use liquid clustering for tables | Databricks on AWS](https://docs.databricks.com/aws/en/delta/clustering)