# Graphalytics Giraph platform extension

[![Build Status](http://jenkins.tribler.org/buildStatus/icon?job=Graphalytics_Giraph_master_tester)](http://jenkins.tribler.org/job/Graphalytics_Giraph_master_tester/)

## Getting started

Please refer to the documentation of the Graphalytics core (`graphalytics` repository) for an introduction to using Graphalytics.


## Giraph-specific benchmark configuration

The `giraph` benchmark runs on Hadoop version 2.4.1 or later (earlier versions have not been attempted) and requires ZooKeeper (tested with 3.4.1). Before launching the benchmark, ensure Hadoop is running in either pseudo-distributed or distributed mode, and ensure that the ZooKeeper service is running. Next, edit `config/platform.properties` in the Graphalytics distribution and change the following settings:

 - `platform.giraph.zoo-keeper-address`: Set to the hostname and port on which ZooKeeper is running.
 - `platform.giraph.job.heap-size`: Set to the amount of heap space (in MB) each worker should have. As Giraph runs on MapReduce, this setting corresponds to the JVM heap specified for each map task, i.e., `mapreduce.map.java.opts`.
 - `platform.giraph.job.memory-size`: Set to the amount of memory (in MB) each worker should have. This corresponds to the amount of memory requested from the YARN resource manager for each worker, i.e., `mapreduce.map.memory.mb`.
 - `platform.giraph.job.worker-count`: Set to an appropriate number of workers for the Hadoop cluster. Note that Giraph launches an additional master process.
 - `platform.hadoop.home`: Set to the root of your Hadoop installation (`$HADOOP_HOME`).

