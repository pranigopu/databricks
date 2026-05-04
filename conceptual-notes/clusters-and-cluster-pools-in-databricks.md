<h1>Clusters & Cluster Pools in Databricks</h1>

---

**Contents**:

- [Cluster Definition](#cluster-definition)
- [Cluster Types](#cluster-types)
  - [All-Purpose Cluster](#all-purpose-cluster)
  - [Job Cluster](#job-cluster)
- [Cluster Pool Definition](#cluster-pool-definition)

---

# Cluster Definition

Cluster: The execution environment, i.e. a set of…

* computation resources  
* resource and execution configurations

… on which you can run notebooks and jobs.

> **Reference**: [Databricks components | Databricks on AWS](https://docs.databricks.com/aws/en/getting-started/concepts)

# Cluster Types

> **Main reference**: [Databricks components | Databricks on AWS](https://docs.databricks.com/aws/en/getting-started/concepts)

## All-Purpose Cluster
Manually-created cluster for general-purpose computation. Properties:

* You can create an all-purpose cluster using UI, CLI, or REST API  
* You can manually terminate and restart an all-purpose cluster  
* Multiple users can share such clusters to do collaborative interactive analysis

## Job Cluster

A cluster created by Databricks job scheduler when you run a job. Properties:

- Creation happens when you run a job on a new job cluster (as an option)  
- Cluster terminates the cluster when the job is complete  
- You cannot restart a job cluster

# Cluster Pool Definition

> **Reference**: [Connect to pools | Databricks on AWS](https://docs.databricks.com/aws/en/compute/pool-index)

Databricks cluster pools (or simply “Databricks pools”) are a set of **idle**, **ready-to-use** instances (see instance definition below). When cluster nodes are created using the idle instances, cluster start and auto-scaling times are reduced. If the pool has no idle instances, the pool expands by allocating a new instance from the instance provider in order to accommodate the cluster's request.

**DEFINITION: Instance**: *In Databricks, an instance primarily refers to a virtual machine (VM) node from the underlying cloud provider (AWS, Azure, or GCP) that provides the raw compute power (CPU, RAM, storage) for running a node of a Spark cluster.*

When a cluster releases an instance, it returns to the pool and is free for another cluster to use. Only clusters attached to a pool can use that pool’s idle instances. ***Databricks does not charge DBUs while instances are idle in the pool. Instance provider billing does apply.*** You can manage pools using the UI or by calling the Instance Pools API (see: [Instance Pools API | REST API reference | Databricks on AWS](https://docs.databricks.com/api/workspace/instancepools)).