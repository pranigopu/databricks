<h1>Broadcast Join in Spark</h1>

---

- [Problem Statement](#problem-statement)
- [Solution](#solution)
- [Usage](#usage)

---

# Problem Statement

Spark uses a master-slave (or driver-executor) distributed system architecture (learn more about Spark’s architecture here: [Distributed Computing with Apache Spark](https://docs.google.com/document/d/1Z52L78aJZBFK-ZFCenMir09MhkrT0fNHAi2k5GUhH64/)). When a large dataset is being processed through Spark, the driver (master node) generally chunks this dataset and distributes these chunks to one or more executors (worker/slave nodes). Moreover, a DataFrame object is itself stored in a distributed manner across the main memories of multiple nodes. Hence, in either case, when we want to join a smaller DataFrame (i.e. a set of columns or a set of rows) to the larger DataFrame currently in use (e.g. if we want to add “discount” and “revenue” columns to a DataFrame containing data on prices and quantities), instead of consolidating and updating the large dataset directly and reshuffling its chunks/partitions across nodes (“shuffling” here is a process within the MapReduce pattern; see “The MapReduce Pattern” in [Distributed Computing with Apache Spark](https://docs.google.com/document/d/1Z52L78aJZBFK-ZFCenMir09MhkrT0fNHAi2k5GUhH64/)).

# Solution

> **References**:
> 
> - [What is Broadcast Join in spark? | Spark Optimization | IN 3 MINUTES | Definition | Applications](https://www.youtube.com/watch?v=cFZPC9DYgOg)  
> [PySpark Broadcast Join with Example](https://sparkbyexamples.com/pyspark/pyspark-broadcast-join-with-example/)

Broadcast join is the process of broadcasting a sufficiently small dataset across executors so they can perform local join operations between this and the chunks/partitions of the large dataset that they hold. ***Broadcast join is an optimisation technique that reduces data shuffling (i.e. the consolidation and re-distribution of data) that occurs during traditional joins (improving performance and out-of-memory errors).*** The flow of broadcast join is as follows:

- Spark loads the smaller dataset (S) into the cluster  
- Spark loads the larger dataset (L) into the cluster  
- Spark checks if S is small enough to be broadcasted <br> *Spark SQL decides this automatically based on a configurable size threshold*  
- If S is within the broadcast threshold, Spark broadcasts it to all executors  
- L, however, is distributed across the executors in chunks/partitions
- Each executor performs join operation using S and the chunk of L it holds
- Results from each executor are collected and combined to get final joined dataset

# Usage

When to use:

- When one of the datasets is small enough to fit into each executor efficiently <br> **NOTE**: *The definition of “small enough” can be configured in Spark*  
- Frequent reuse of the smaller dataset for join operations <br> I.e.: *If the smaller dataset needs to be joined with multiple larger datasets*

When not to use:

- When both datasets are too large <br> **NOTE**: *It can lead to memory overhead or out-of-memory errors*  
- High variability in executor memory availability <br> **NOTE**: *This can make it problematic to broadcast somewhat larger datasets*