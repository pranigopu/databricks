<h1>Data Skew & Salting</h1>

---

**Contents**:

- [Data Skew](#data-skew)
- [Salting (Spark)](#salting-spark)

---

# Data Skew

Data skew is a situation in data distribution wherein  data is unevenly distributed across partitions/keys/nodes in a dataset (terminology here depends on the kind of distributed system and use-case in question). This causes some processing tasks to take significantly longer than others (since the slower-processing tasks may deal with heavier partitions), leading to:

- Performance bottlenecks (heavy partition-handling jobs becoming bottlenecks)  
- Resource underutilisation (wasted partitions/nodes)  
- Potential system crashes (due to memory overload or computational limits)

# Salting (Spark)

Real-world data is often distributed unevenly. Some keys may have a high number of occurrences (hot keys), causing a skewed distribution of data across partitions. Salting is a technique to eliminate data skew by appending random unique values (salt) to the hot keys. This process ensures that the hot keys are broken down into a number of keys, resulting in unevenly distributed partitions eventually evening out as data is distributed more evenly across partitions/nodes.

**NOTE**: *A hot key (or skewed key) refers to a specific partition key in a dataset that receives a disproportionately high volume of data compared to other keys.*

> **References**:
> 
> - [Handling Data Skew in Spark: The Power of Salting \- CloudThat Resources](https://www.cloudthat.com/resources/blog/handling-data-skew-in-spark-the-power-of-salting)
> - [Understanding Salting in Spark: A Practical Guide | by Ajaykumar Dev | Medium](https://medium.com/@nikaljeajay36/understanding-salting-in-spark-a-practical-guide-bf30f4525f64)
> - [How to: Handling Data Skewness in Apache Spark | by Ankush Singh | Medium](https://medium.com/@diehardankush/how-to-understanding-data-skewness-in-apache-spark-9e93b9a68f46) <br> (this mentions and defines “hot key”)