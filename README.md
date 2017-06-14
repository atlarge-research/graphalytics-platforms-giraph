# Graphalytics Giraph platform driver

[![Build Status](http://jenkins.tribler.org/buildStatus/icon?job=Graphalytics_Giraph_master_tester)](http://jenkins.tribler.org/job/Graphalytics_Giraph_master_tester/)

[Apache Giraph](http://giraph.apache.org) is an iterative graph processing system built for high scalability, originated as the open-source counterpart to Google's Pregel, inspired by the Bulk Synchronous Parallel model of distributed computation introduced by Leslie Valiant.

To execute Graphalytics benchmark on Giraph, follow the steps in the Graphalytics tutorial on [Running Benchmark](https://github.com/ldbc/ldbc_graphalytics/wiki/Manual%3A-Running-Benchmark) with the Giraph-specific instructions listed below.

### Obtain the platform driver
There are two possible ways to obtain the Giraph platform driver:

 1. **Download the (prebuild) **[Giraph platform driver](https://atlarge-research.com/projects/graphalytics/platforms)** distribution from our website.

 2. **Build the platform drivers**: 
  - (To be deprecated): Current it is required to build the [Graphlytics core libraries](https://github.com/ldbc/ldbc_graphalytics/tree/) with ``mvn clean install -Pgranula``, soon it will be available via Maven central repo.
  - Download the source code from this repository.
  - Execute `mvn clean package` in the root directory (See more details in [Software Build](https://github.com/ldbc/ldbc_graphalytics/wiki/Documentation:-Software-Build)).
  - Extract the distribution from `graphalytics-{graphalytics-version}-giraph-{platform-version}.tar.gz`.

### Verify the necessary prerequisites
The softwares listed below are required by the Giraph platform driver, which must be available in the cluster environment. Softwares that are provided are already included in the platform driver.

| Software | Version (tested) | Usage | Description | Provided |
|-------------|:-------------:|:-------------:|:-------------:|:-------------:|
| Giraph | 1.6.0 | Platform| Providing Giraph implementation | ✔(maven) |
| Graphalytics | 1.0 (TODO) | Driver | Graphalytics benchmark suite | ✔(maven) |
| Granula | 0.1 (TODO) | Driver | Fine-grained performance analysis | ✔(maven) |
| YARN | 2.6.1 | Deployment | Job provisioning and allocation | - |
| Zookeeper | 3.4.1 | Deployment | Synchronizing Giraph workers | - |
| JDK | 1.7.0+ | Build | Java virtual machine | - |
| Maven | 3.3.9 | Build | Building the platform driver | - |

 - `Yarn`: should be reachable in the compute node where the benchmark will be executed.
 - `Zookeeper`: should be running in a compute node accessible via the network.

### Adjust the benchmark configurations
Adjust the Giraph configurations in `config/platform.properties`: 

 - `platform.giraph.zoo-keeper-address`: Set to the hostname and port on which ZooKeeper is running.
 - `platform.giraph.job.heap-size`: Set to the amount of heap space (in MB) each worker should have. As Giraph runs on MapReduce, this setting corresponds to the JVM heap specified for each map task, i.e., `mapreduce.map.java.opts`.
 - `platform.giraph.job.memory-size`: Set to the amount of memory (in MB) each worker should have. This corresponds to the amount of memory requested from the YARN resource manager for each worker, i.e., `mapreduce.map.memory.mb`.
 - `platform.giraph.job.worker-count`: Set to an appropriate number of workers for the Hadoop cluster. Note that Giraph launches an additional master process.
 - `platform.hadoop.home`: Set to the root of your Hadoop installation (`$HADOOP_HOME`).

