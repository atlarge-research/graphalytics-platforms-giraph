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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base class for all jobs in the Giraph benchmark suite. Configures and executes
 * a Giraph job using the computation and vertex format specified by subclasses
 * of GiraphJob. In addition, a pre-execution hook is provided to enable arbitrary
 * job-specific configuration to be added to the job.
 *
 * @author Tim Hegeman
 */
public abstract class GiraphJob extends Configured implements Tool {
	private static final Logger LOG = LogManager.getLogger();

	/**
	 * The configuration key for the JVM heap size of Giraph workers in megabytes.
	 */
	public static final String HEAP_SIZE_MB_KEY = "graphalytics.giraphjob.heap-size-mb";
	/**
	 * The JVM heap size of Giraph workers in megabytes.
	 */
	public static final IntConfOption HEAP_SIZE_MB = new IntConfOption(HEAP_SIZE_MB_KEY,
			768, "JVM heap size of Giraph workers in megabytes");

	/**
	 * The configuration key for the Giraph worker memory size in megabytes.
	 */
	public static final String WORKER_MEMORY_MB_KEY = "graphalytics.giraphjob.worker-memory-mb";
	/**
	 * The Giraph worker memory size in megabytes.
	 */
	public static final IntConfOption WORKER_MEMORY_MB = new IntConfOption(WORKER_MEMORY_MB_KEY,
			1024, "Giraph worker memory size in megabytes");

	/**
	 * The configuration key for the number of Giraph workers to used.
	 */
	public static final String WORKER_COUNT_KEY = "graphalytics.giraphjob.worker-count";
	/**
	 * The number of Giraph workers to be used.
	 */
	public static final IntConfOption WORKER_COUNT = new IntConfOption(WORKER_COUNT_KEY,
			1, "Number of Giraph workers to use");

	/**
	 * The configuration key for the input path of the Giraph job.
	 */
	public static final String INPUT_PATH_KEY = "graphalytics.giraphjob.input-path";
	/**
	 * The input path of the Giraph job.
	 */
	public static final StrConfOption INPUT_PATH = new StrConfOption(INPUT_PATH_KEY,
			"", "Giraph input path");

	/**
	 * The configuration key for the output path of the Giraph job.
	 */
	public static final String OUTPUT_PATH_KEY = "graphalytics.giraphjob.output-path";
	/**
	 * The output path of the Giraph job.
	 */
	public static final StrConfOption OUTPUT_PATH = new StrConfOption(OUTPUT_PATH_KEY,
			"", "Giraph output path");

	/**
	 * The configuration key for the ZooKeeper address used by Giraph.
	 */
	public static final String ZOOKEEPER_ADDRESS_KEY = "graphalytics.giraphjob.zookeeper-address";
	/**
	 * The ZooKeeper address used by Giraph (hostname:port).
	 */
	public static final StrConfOption ZOOKEEPER_ADDRESS = new StrConfOption(ZOOKEEPER_ADDRESS_KEY,
			"", "ZooKeeper address");

	public static final String JOB_ID_KEY = "graphalytics.job-id";

	public static final StrConfOption JOB_ID = new StrConfOption(JOB_ID_KEY,
			"", "Job Id");

	private String inputPath;
	private String outputPath;
	private String zooKeeperAddress;
	private int workerCount;
	private int heapSize;
	private int workerMemory;

	/**
	 * @return the Giraph job output path
	 */
	protected String getOutputPath() {
		return outputPath;
	}

	private void loadConfiguration() {
		if (INPUT_PATH.isDefaultValue(getConf())) {
			throw new IllegalStateException("Missing mandatory configuration: " + INPUT_PATH_KEY);
		}
		if (OUTPUT_PATH.isDefaultValue(getConf())) {
			throw new IllegalStateException("Missing mandatory configuration: " + OUTPUT_PATH_KEY);
		}
		if (ZOOKEEPER_ADDRESS.isDefaultValue(getConf())) {
			throw new IllegalStateException("Missing mandatory configuration: " + ZOOKEEPER_ADDRESS_KEY);
		}

		workerCount = WORKER_COUNT.get(getConf());
		heapSize = HEAP_SIZE_MB.get(getConf());
		workerMemory = WORKER_MEMORY_MB.get(getConf());
		inputPath = INPUT_PATH.get(getConf());
		outputPath = OUTPUT_PATH.get(getConf());
		zooKeeperAddress = ZOOKEEPER_ADDRESS.get(getConf());
	}

	/**
	 * Creates a new Giraph job configuration and loads it with generic options
	 * such as input and output paths, the number of workers, and the worker
	 * heap size. It sets the computation and I/O format classes based on
	 * the return value of their respective hooks. The configure method is called
	 * to allow for job-specific configuration. Finally, the Giraph job is
	 * submitted and is executed (blocking).
	 *
	 * @param args ignored
	 * @return zero iff the job completed successfully
	 */
	@Override
	public final int run(String[] args) throws Exception {
		loadConfiguration();

		// Prepare the job configuration
		GiraphConfiguration configuration = new GiraphConfiguration(getConf());

		// Set the computation class
		configuration.setComputationClass(getComputationClass());

		// Prepare input paths
		Path vertexInputPath = new Path(inputPath + ".v");
		Path edgeInputPath = new Path(inputPath + ".e");

		// Set input paths
		GiraphFileInputFormat.addVertexInputPath(configuration, vertexInputPath);
		GiraphFileInputFormat.addEdgeInputPath(configuration, edgeInputPath);

		// Set vertex/edge input format class
		configuration.setVertexInputFormatClass(getVertexInputFormatClass());
		configuration.setEdgeInputFormatClass(getEdgeInputFormatClass());

		// Set and output path and class
		configuration.set(FileOutputFormat.OUTDIR, outputPath);
		if (getVertexOutputFormatClass() != null) {
			configuration.setVertexOutputFormatClass(getVertexOutputFormatClass());
		} else {
			configuration.setEdgeOutputFormatClass(getEdgeOutputFormatClass());
		}

		// Set deployment-specific configuration from external configuration files
		configuration.setWorkerConfiguration(workerCount, workerCount, 100.0f);
		configuration.setZooKeeperConfiguration(zooKeeperAddress);
		configuration.setInt("mapreduce.map.memory.mb", workerMemory);
		configuration.set("mapreduce.map.java.opts", "-Xmx" + heapSize + "M");

		// Set algorithm-specific configuration
		configure(configuration);

		// Create the Giraph job
		org.apache.giraph.job.GiraphJob job = new org.apache.giraph.job.GiraphJob(
				configuration, "Graphalytics: " + getClass().getSimpleName());
		// Launch it
		LOG.debug("- Starting Giraph job");
		return job.run(false) ? 0 : -1;
	}

	/**
	 * Hook for subclasses of GiraphJob to specify which Computation to run for
	 * the Giraph job.
	 *
	 * @return a job-specific Computation class
	 */
	@SuppressWarnings("rawtypes")
	protected abstract Class<? extends Computation> getComputationClass();

	/**
	 * Hook for subclasses of GiraphJob to specify which input format to use
	 * when importing vertices into Giraph.
	 *
	 * @return a job-specific VertexInputFormat
	 */
	@SuppressWarnings("rawtypes")
	protected abstract Class<? extends VertexInputFormat> getVertexInputFormatClass();

	/**
	 * Hook for subclasses of GiraphJob to specify which output format to use
	 * when storing the resulting vertices of a Giraph computation.
	 *
	 * @return a job-specific VertexOutputFormat
	 */
	@SuppressWarnings("rawtypes")
	protected abstract Class<? extends VertexOutputFormat> getVertexOutputFormatClass();

	/**
	 * Hook for subclasses of GiraphJob to specify which input format to use
	 * when importing edges into Giraph.
	 *
	 * @return a job-specific VertexInputFormat
	 */
	@SuppressWarnings("rawtypes")
	protected abstract Class<? extends EdgeInputFormat> getEdgeInputFormatClass();

	/**
	 * Hook for subclasses of GiraphJob to specify which output format to use
	 * when storing the resulting edges of a Giraph computation.
	 *
	 * @return a job-specific VertexOutputFormat
	 */
	@SuppressWarnings("rawtypes")
	protected abstract Class<? extends EdgeOutputFormat> getEdgeOutputFormatClass();

	/**
	 * Hook for subclasses of GiraphJob to set arbitrary configuration for the
	 * Giraph job. Often used to set global parameters needed for a specific
	 * graph processing algorithm.
	 *
	 * @param config the Giraph configuration for this job
	 */
	protected abstract void configure(GiraphConfiguration config);

}
