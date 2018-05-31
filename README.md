# Graphalytics Giraph platform driver

[![Build Status](https://jenkins.tribler.org/buildStatus/icon?job=Graphalytics/Platforms/Giraph_master)](https://jenkins.tribler.org/job/Graphalytics/job/Platforms/job/Giraph_master/)

### Getting started
This is a [Graphalytics](https://github.com/ldbc/ldbc_graphalytics/) benchmark driver for [Apache Giraph](http://giraph.apache.org). Giraph is an iterative graph processing system built for high scalability, originated as the open-source counterpart to Google's Pregel, inspired by the Bulk Synchronous Parallel model of distributed computation introduced by Leslie Valiant.

  - Make sure that you have [installed Graphalytics](https://github.com/ldbc/ldbc_graphalytics/wiki/Documentation%3A-Software-Build#the-core-repository). 
  - Download the source code from this repository.
  - Execute `mvn clean package` in the root directory (See more details in [Software Build](https://github.com/ldbc/ldbc_graphalytics/wiki/Documentation:-Software-Build)).
  - Extract the distribution from `graphalytics-{graphalytics-version}-giraph-{platform-version}.tar.gz`.

### Verify the necessary prerequisites
The softwares listed below are required by the Giraph platform driver, which must be available in the cluster environment. Softwares that are provided are already included in the platform driver.

| Software | Version (tested) | Usage | Description | Provided |
|-------------|:-------------:|:-------------:|:-------------:|:-------------:|
| Giraph | 1.2.0 | Platform| Providing Giraph implementation | ✔(maven) |
| Graphalytics | 1.0 | Driver | Graphalytics benchmark suite | ✔(maven) |
| Granula | 1.0 | Driver | Fine-grained performance analysis | ✔(maven) |
| YARN | 2.6.1 | Deployment | Job provisioning and allocation | - |
| Zookeeper | 3.4.1 | Deployment | Synchronizing Giraph workers | - |
| JDK | 1.7+ | Build | Java virtual machine | - |
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

### Known Issues
* Benchmark reports will report `nan` as processing time when yarn log aggregation is off. The solution is to enable log aggregation in the `yarn-site.xml` file by setting `yarn.log-aggregation-enable` to true.

### Running a benchmark

To execute a Graphalytics benchmark on Giraph (using this driver), follow the steps in the Graphalytics tutorial on [Running Benchmark](https://github.com/ldbc/ldbc_graphalytics/wiki/Manual%3A-Running-Benchmark).
