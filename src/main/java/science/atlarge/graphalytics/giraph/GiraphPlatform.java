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
package science.atlarge.graphalytics.giraph;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nl.tudelft.granula.archiver.PlatformArchive;
import nl.tudelft.granula.modeller.job.JobModel;
import nl.tudelft.granula.modeller.platform.Giraph;
import nl.tudelft.granula.util.FileUtil;
import science.atlarge.graphalytics.domain.graph.FormattedGraph;
import science.atlarge.graphalytics.report.result.BenchmarkMetric;
import science.atlarge.graphalytics.report.result.BenchmarkMetrics;
import science.atlarge.graphalytics.domain.algorithms.Algorithm;
import science.atlarge.graphalytics.report.result.BenchmarkRunResult;
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.giraph.log.JobLogger;
import science.atlarge.graphalytics.granula.GranulaAwarePlatform;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import science.atlarge.graphalytics.execution.PlatformExecutionException;
import science.atlarge.graphalytics.configuration.ConfigurationUtil;
import science.atlarge.graphalytics.configuration.InvalidConfigurationException;
import science.atlarge.graphalytics.giraph.algorithms.bfs.BreadthFirstSearchJob;
import science.atlarge.graphalytics.giraph.algorithms.cdlp.CommunityDetectionLPJob;
import science.atlarge.graphalytics.giraph.algorithms.ffm.ForestFireModelJob;
import science.atlarge.graphalytics.giraph.algorithms.lcc.LocalClusteringCoefficientJob;
import science.atlarge.graphalytics.giraph.algorithms.pr.PageRankJob;
import science.atlarge.graphalytics.giraph.algorithms.sssp.SingleSourceShortestPathJob;
import science.atlarge.graphalytics.giraph.algorithms.wcc.WeaklyConnectedComponentsJob;
import org.json.simple.JSONObject;

/**
 * Entry point of the Graphalytics benchmark for Giraph. Provides the platform
 * API required by the Graphalytics core to perform operations such as uploading
 * graphs and executing specific algorithms on specific graphs.
 *
 * @author Tim Hegeman
 */
public class GiraphPlatform implements GranulaAwarePlatform {
	private static final Logger LOG = LogManager.getLogger();

	/**
	 * Default file name for the file storing Giraph properties.
	 */
	public static final String GIRAPH_PROPERTIES_FILE = "platform.properties";

	/**
	 * Default file name for the file storing System properties.
	 */
	public static final String BENCHMARK_PROPERTIES_FILE = "benchmark.properties";

	/**
	 * Property key for setting the number of workers to be used for running Giraph jobs.
	 */
	public static final String JOB_WORKERCOUNT = "platform.giraph.job.worker-count";
	/**
	 * Property key for setting the memory size of each Giraph worker.
	 */
	public static final String JOB_MEMORYSIZE = "platform.giraph.job.memory-size";
	/**
	 * Property key for setting the heap size of each Giraph worker.
	 */
	public static final String JOB_HEAPSIZE = "platform.giraph.job.heap-size";
	/**
	 * Property key for setting the core count of each Giraph worker.
	 */
	public static final String JOB_CORES = "platform.giraph.job.worker-cores";
	/**
	 * Property key for the address of a ZooKeeper instance to use during the benchmark.
	 */
	public static final String ZOOKEEPERADDRESS = "platform.giraph.zoo-keeper-address";
	/**
	 * Property key for the directory on HDFS in which to store all input and output.
	 */
	public static final String HDFS_DIRECTORY_KEY = "platform.hadoop.hdfs.directory";
	/**
	 * Property key for the directory on HDFS in which to store all input and output.
	 */
	public static final String HDFS_DIRECTORY = "graphalytics";

	private Map<String, String> pathsOfGraphs = new HashMap<>();
	private org.apache.commons.configuration.Configuration benchmarkConfig;
	private String hdfsDirectory;

	/**
	 * Constructor that opens the Giraph-specific properties file for the public
	 * API implementation to use.
	 */
	public GiraphPlatform() {
		loadConfiguration();
	}


	@Override
	public void verifySetup() {

	}

	@Override
	public void loadGraph(FormattedGraph formattedGraph) throws Exception {
		LOG.info("Uploading graph \"{}\" to HDFS", formattedGraph.getName());

		String uploadPath = Paths.get(hdfsDirectory, getPlatformName(), "input", formattedGraph.getName()).toString();

		// Upload the graph to HDFS
		FileSystem fs = FileSystem.get(new Configuration());

		LOG.debug("- Uploading vertex list");
		fs.copyFromLocalFile(new Path(formattedGraph.getVertexFilePath()), new Path(uploadPath + ".v"));

		LOG.debug("- Uploading edge list");
		fs.copyFromLocalFile(new Path(formattedGraph.getEdgeFilePath()), new Path(uploadPath + ".e"));

		fs.close();

		// Track available datasets in a map
		pathsOfGraphs.put(formattedGraph.getName(), uploadPath);
	}

	@Override
	public void deleteGraph(FormattedGraph formattedGraph) {
		String path = pathsOfGraphs.get(formattedGraph.getName());

		try(FileSystem fs = FileSystem.get(new Configuration())) {
			fs.delete(new Path(path + ".v"), true);
			fs.delete(new Path(path + ".e"), true);
		} catch(IOException e) {
			LOG.warn("Error occured while deleting files", e);
		}
	}

	private void setupGraph(FormattedGraph formattedGraph) {
		String uploadPath = Paths.get(hdfsDirectory, getPlatformName(), "input", formattedGraph.getName()).toString();
		pathsOfGraphs.put(formattedGraph.getName(), uploadPath);
	}

	@Override
	public void prepare(BenchmarkRun benchmarkRun) {

	}

	@Override
	public void startup(BenchmarkRun benchmark) {
		JobLogger.stopCoreLogging();
		LOG.info(String.format("Logging path at: %s", benchmark.getLogDir().resolve("platform").resolve("driver.logs")));
		JobLogger.startPlatformLogging(benchmark.getLogDir().resolve("platform").resolve("driver.logs"));
	}

	@Override
	public void run(BenchmarkRun benchmark) throws PlatformExecutionException {
		Algorithm algorithm = benchmark.getAlgorithm();
		FormattedGraph formattedGraph = benchmark.getFormattedGraph();
		Object parameters = benchmark.getAlgorithmParameters();

		setupGraph(formattedGraph);

		LOG.info("Executing algorithm \"{}\" on graph \"{}\".", algorithm.getName(), formattedGraph.getName());

		int result;
		try {
			// Prepare the appropriate job for the given algorithm type
			GiraphJob job;
			switch (algorithm) {
				case BFS:
					job = new BreadthFirstSearchJob(parameters, formattedGraph);
					break;
				case CDLP:
					job = new CommunityDetectionLPJob(parameters, formattedGraph);
					break;
				case WCC:
					job = new WeaklyConnectedComponentsJob(formattedGraph);
					break;
				case FFM:
					job = new ForestFireModelJob(parameters, formattedGraph);
				case LCC:
					job = new LocalClusteringCoefficientJob(formattedGraph);
					break;
				case PR:
					job = new PageRankJob(parameters, formattedGraph);
					break;
				case SSSP:
					job = new SingleSourceShortestPathJob(parameters, formattedGraph);
					break;
				default:
					throw new IllegalArgumentException("Unsupported algorithm: " + algorithm);
			}

			// Create the job configuration using the Giraph properties file
			String hdfsOutputPath = Paths.get(hdfsDirectory, getPlatformName(), "output",
					benchmark.getId() + "_" + algorithm.getAcronym() + "-" + formattedGraph.getName()).toString();
			Configuration jobConf = new Configuration();

			GiraphJob.INPUT_PATH.set(jobConf, pathsOfGraphs.get(formattedGraph.getName()));
			GiraphJob.OUTPUT_PATH.set(jobConf, hdfsOutputPath);
			GiraphJob.ZOOKEEPER_ADDRESS.set(jobConf, ConfigurationUtil.getString(benchmarkConfig, ZOOKEEPERADDRESS));

			transferIfSet(benchmarkConfig, JOB_WORKERCOUNT, jobConf, GiraphJob.WORKER_COUNT);
			transferIfSet(benchmarkConfig, JOB_MEMORYSIZE, jobConf, GiraphJob.WORKER_MEMORY_MB);
			transferIfSet(benchmarkConfig, JOB_HEAPSIZE, jobConf, GiraphJob.WORKER_HEAP_MB);
			transferIfSet(benchmarkConfig, JOB_CORES, jobConf, GiraphJob.WORKER_CORES);

			GiraphJob.JOB_ID.set(jobConf, benchmark.getId());

			transferGiraphOptions(benchmarkConfig, jobConf);

			// Execute the Giraph job
			result = ToolRunner.run(jobConf, job, new String[0]);
			// TODO: Clean up intermediate and output data, depending on some configuration.

			if(benchmark.isOutputRequired()){
					FileSystem fs = FileSystem.get(new Configuration());
					fs.copyToLocalFile(false, new Path(hdfsOutputPath),
							new Path(benchmark.getOutputDir().toAbsolutePath().toString()), true);
					fs.close();
			}
			deleteOutput(hdfsOutputPath);

		} catch (Exception e) {
			throw new PlatformExecutionException("Giraph job failed with exception: ", e);
		}

		if (result != 0) {
			throw new PlatformExecutionException("Giraph job completed with exit code = " + result);
		}

	}


	@Override
	public BenchmarkMetrics finalize(BenchmarkRun benchmarkRun) {
		JobLogger.stopPlatformLogging();
		JobLogger.startCoreLogging();
		JobLogger.collectYarnLogs(benchmarkRun.getLogDir());
		LOG.info("Extracting performance metrics from logs.");
		java.nio.file.Path platformLogPath = benchmarkRun.getLogDir().resolve("platform");

		final List<Double> superstepTimes = new ArrayList<>();

		try {
			Files.walkFileTree(platformLogPath, new SimpleFileVisitor<java.nio.file.Path>() {
				@Override
				public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) throws IOException {
					String logs = FileUtil.readFile(file);

					LOG.info(String.format("Parsing logs at %s.", file.toAbsolutePath()));
					for (String line : logs.split("\n")) {
						if (line.contains("MasterThread  - superstep")) {
							Pattern regex = Pattern.compile(
									".*MasterThread  - superstep (\\d*): Took ([+-]?([0-9]*[.])?[0-9]+) seconds..*");
							Matcher matcher = regex.matcher(line);
							matcher.find();
							superstepTimes.add(Double.parseDouble(matcher.group(2)));

							LOG.info(String.format("Extracting performance metrics from superstep %s -> %s s", matcher.group(1), matcher.group(2)));
						}
					}
					return FileVisitResult.CONTINUE;
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (superstepTimes.size() != 0) {
			Double procTime = 0.0;
			for (Double superstepTime : superstepTimes) {
				procTime += superstepTime;
			}

			BenchmarkMetrics metrics = new BenchmarkMetrics();
			BigDecimal procTimeS = (new BigDecimal(procTime)).setScale(3, RoundingMode.CEILING);
			metrics.setProcessingTime(new BenchmarkMetric(procTimeS, "s"));

			return metrics;
		} else {
			LOG.error("Failed to find any metrics regarding superstep runtime.");
			return new BenchmarkMetrics();
		}
	}

	@Override
	public void enrichMetrics(BenchmarkRunResult benchmarkRunResult, java.nio.file.Path arcDirectory) {
		try {
			PlatformArchive platformArchive = PlatformArchive.readArchive(arcDirectory);
			JSONObject processGraph = platformArchive.operation("Execute");
			BenchmarkMetrics metrics = benchmarkRunResult.getMetrics();

			Integer procTimeMS = Integer.parseInt(platformArchive.info(processGraph, "Duration"));
			BigDecimal procTimeS = (new BigDecimal(procTimeMS)).divide(new BigDecimal(1000), 3, BigDecimal.ROUND_CEILING);
			metrics.setProcessingTime(new BenchmarkMetric(procTimeS, "s"));
		} catch(Exception e) {
			LOG.error("Failed to enrich metrics.");
		}
	}

	@Override
	public void terminate(BenchmarkRun benchmark) {

	}

	private void loadConfiguration() {
		// Load Giraph-specific configuration
		try {
			benchmarkConfig = ConfigurationUtil.loadConfiguration(BENCHMARK_PROPERTIES_FILE);
		} catch (InvalidConfigurationException e) {
			// Fall-back to an empty properties file
			LOG.info("Could not find or load benchmark.properties.");
			benchmarkConfig = new PropertiesConfiguration();
		}
		hdfsDirectory = benchmarkConfig.getString(HDFS_DIRECTORY_KEY, HDFS_DIRECTORY);
	}


	private void deleteOutput(String outputPath) {

		try(FileSystem fs = FileSystem.get(new Configuration())) {
			LOG.info(String.format("Deleting output directory: %s at hdfs.", outputPath));
			fs.delete(new Path(outputPath), true);
		} catch(IOException e) {
			LOG.warn("Error occured while deleting files", e);
		}
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
		org.apache.commons.configuration.Configuration giraphOptions = source.subset("platform.giraph.options");
		for (Iterator<String> optionIterator = giraphOptions.getKeys(); optionIterator.hasNext(); ) {
			String option = optionIterator.next();
			destination.set("giraph." + option, giraphOptions.getString(option));
		}
	}

	@Override
	public JobModel getJobModel() {
		return new JobModel(new Giraph());
	}

	@Override
	public String getPlatformName() {
		return "giraph";
	}

}
