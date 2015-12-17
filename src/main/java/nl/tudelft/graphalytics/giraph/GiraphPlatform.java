/*
 * Copyright 2015 Delft University of Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.tudelft.graphalytics.giraph;

import nl.tudelft.graphalytics.Platform;
import nl.tudelft.graphalytics.PlatformExecutionException;
import nl.tudelft.graphalytics.configuration.ConfigurationUtil;
import nl.tudelft.graphalytics.configuration.InvalidConfigurationException;
import nl.tudelft.graphalytics.domain.*;
import nl.tudelft.graphalytics.giraph.algorithms.bfs.BreadthFirstSearchJob;
import nl.tudelft.graphalytics.giraph.algorithms.cd.CommunityDetectionJob;
import nl.tudelft.graphalytics.giraph.algorithms.conn.ConnectedComponentsJob;
import nl.tudelft.graphalytics.giraph.algorithms.evo.ForestFireModelJob;
import nl.tudelft.graphalytics.giraph.algorithms.pr.PageRankJob;
import nl.tudelft.graphalytics.giraph.algorithms.stats.LocalClusteringCoefficientJob;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Entry point of the Graphalytics benchmark for Giraph. Provides the platform
 * API required by the Graphalytics core to perform operations such as uploading
 * graphs and executing specific algorithms on specific graphs.
 *
 * @author Tim Hegeman
 */
public class GiraphPlatform implements Platform {
	private static final Logger LOG = LogManager.getLogger();

	/**
	 * Default file name for the file storing Giraph properties.
	 */
	public static final String GIRAPH_PROPERTIES_FILE = "giraph.properties";

	/**
	 * Property key for setting the number of workers to be used for running Giraph jobs.
	 */
	public static final String JOB_WORKERCOUNT = "giraph.job.worker-count";
	/**
	 * Property key for setting the heap size of each Giraph worker.
	 */
	public static final String JOB_HEAPSIZE = "giraph.job.heap-size";
	/**
	 * Property key for setting the memory size of each Giraph worker.
	 */
	public static final String JOB_MEMORYSIZE = "giraph.job.memory-size";
	/**
	 * Property key for the address of a ZooKeeper instance to use during the benchmark.
	 */
	public static final String ZOOKEEPERADDRESS = "giraph.zoo-keeper-address";
	/**
	 * Property key for the directory on HDFS in which to store all input and output.
	 */
	public static final String HDFS_DIRECTORY_KEY = "hadoop.hdfs.directory";
	/**
	 * Property key for the directory on HDFS in which to store all input and output.
	 */
	public static final String HDFS_DIRECTORY = "graphalytics";

	private Map<String, String> pathsOfGraphs = new HashMap<>();
	private org.apache.commons.configuration.Configuration giraphConfig;
	private String hdfsDirectory;

	/**
	 * Constructor that opens the Giraph-specific properties file for the public
	 * API implementation to use.
	 */
	public GiraphPlatform() {
		loadConfiguration();
	}

	private void loadConfiguration() {
		// Load Giraph-specific configuration
		try {
			giraphConfig = new PropertiesConfiguration(GIRAPH_PROPERTIES_FILE);
		} catch (ConfigurationException e) {
			// Fall-back to an empty properties file
			LOG.info("Could not find or load giraph.properties.");
			giraphConfig = new PropertiesConfiguration();
		}
		hdfsDirectory = giraphConfig.getString(HDFS_DIRECTORY_KEY, HDFS_DIRECTORY);
	}

	@Override
	public void uploadGraph(Graph graph) throws Exception {
		LOG.info("Uploading graph \"{}\" to HDFS", graph.getName());

		String uploadPath = Paths.get(hdfsDirectory, getName(), "input", graph.getName()).toString();

		// Upload the graph to HDFS
		FileSystem fs = FileSystem.get(new Configuration());
		LOG.debug("- Uploading vertex list");
		fs.copyFromLocalFile(new Path(graph.getVertexFilePath()), new Path(uploadPath + ".v"));
		LOG.debug("- Uploading edge list");
		fs.copyFromLocalFile(new Path(graph.getEdgeFilePath()), new Path(uploadPath + ".e"));
		fs.close();

		// Track available datasets in a map
		pathsOfGraphs.put(graph.getName(), uploadPath);
	}

	@Override
	public PlatformBenchmarkResult executeAlgorithmOnGraph(Benchmark benchmark) throws PlatformExecutionException {
		Algorithm algorithm = benchmark.getAlgorithm();
		Graph graph = benchmark.getGraph();
		Object parameters = benchmark.getAlgorithmParameters();

		LOG.info("Executing algorithm \"{}\" on graph \"{}\".", algorithm.getName(), graph.getName());

		int result;
		try {
			// Prepare the appropriate job for the given algorithm type
			GiraphJob job;
			switch (algorithm) {
				case BFS:
					job = new BreadthFirstSearchJob(parameters, graph.getGraphFormat());
					break;
				case CD:
					job = new CommunityDetectionJob(parameters, graph.getGraphFormat());
					break;
				case CONN:
					job = new ConnectedComponentsJob(graph.getGraphFormat());
					break;
				case EVO:
					job = new ForestFireModelJob(parameters, graph.getGraphFormat());
					break;
				case STATS:
					job = new LocalClusteringCoefficientJob(graph.getGraphFormat());
					break;
				case PAGERANK:
					job = new PageRankJob(parameters, graph.getGraphFormat());
					break;
				default:
					throw new IllegalArgumentException("Unsupported algorithm: " + algorithm);
			}

			// Create the job configuration using the Giraph properties file
			String hdfsOutputPath = Paths.get(hdfsDirectory, getName(), "output",
					algorithm + "-" + graph.getName()).toString();
			Configuration jobConf = new Configuration();
			GiraphJob.INPUT_PATH.set(jobConf, pathsOfGraphs.get(graph.getName()));
			GiraphJob.OUTPUT_PATH.set(jobConf, hdfsOutputPath);
			GiraphJob.ZOOKEEPER_ADDRESS.set(jobConf, ConfigurationUtil.getString(giraphConfig, ZOOKEEPERADDRESS));
			transferIfSet(giraphConfig, JOB_WORKERCOUNT, jobConf, GiraphJob.WORKER_COUNT);
			transferIfSet(giraphConfig, JOB_HEAPSIZE, jobConf, GiraphJob.HEAP_SIZE_MB);
			transferIfSet(giraphConfig, JOB_MEMORYSIZE, jobConf, GiraphJob.WORKER_MEMORY_MB);

			transferGiraphOptions(giraphConfig, jobConf);

			// Execute the Giraph job
			result = ToolRunner.run(jobConf, job, new String[0]);
			// TODO: Clean up intermediate and output data, depending on some configuration.
		} catch (Exception e) {
			throw new PlatformExecutionException("Giraph job failed with exception: ", e);
		}

		if (result != 0) {
			throw new PlatformExecutionException("Giraph job completed with exit code = " + result);
		}
		return new PlatformBenchmarkResult(NestedConfiguration.empty());
	}

	private void transferIfSet(org.apache.commons.configuration.Configuration source, String sourceProperty,
			Configuration destination, IntConfOption destinationOption) throws InvalidConfigurationException {
		if (source.containsKey(sourceProperty)) {
			destinationOption.set(destination, ConfigurationUtil.getInteger(source, sourceProperty));
		} else {
			LOG.warn(sourceProperty + " is not configured, defaulting to " +
					destinationOption.getDefaultValue() + ".");
		}
	}

	private static void transferGiraphOptions(org.apache.commons.configuration.Configuration source,
			Configuration destination) {
		org.apache.commons.configuration.Configuration giraphOptions = source.subset("giraph.options");
		for (Iterator<String> optionIterator = giraphOptions.getKeys(); optionIterator.hasNext(); ) {
			String option = optionIterator.next();
			destination.set("giraph." + option, giraphOptions.getString(option));
		}
	}

	@Override
	public void deleteGraph(String graphName) {
		// TODO: Clean up the graph on HDFS, depending on some configuration.
	}

	@Override
	public String getName() {
		return "giraph";
	}

	@Override
	public NestedConfiguration getPlatformConfiguration() {
		try {
			org.apache.commons.configuration.Configuration configuration =
					new PropertiesConfiguration(GIRAPH_PROPERTIES_FILE);
			return NestedConfiguration.fromExternalConfiguration(configuration, GIRAPH_PROPERTIES_FILE);
		} catch (ConfigurationException ex) {
			return NestedConfiguration.empty();
		}
	}

}
