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

import nl.tudelft.granula.archiver.PlatformArchive;
import nl.tudelft.granula.modeller.job.JobModel;
import nl.tudelft.granula.modeller.platform.Giraph;
import nl.tudelft.graphalytics.BenchmarkMetrics;
import nl.tudelft.graphalytics.domain.Benchmark;
import nl.tudelft.graphalytics.domain.BenchmarkResult;
import nl.tudelft.graphalytics.granula.GranulaAwarePlatform;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import java.nio.file.Path;

/**
 * Giraph platform integration for the Graphalytics benchmark.
 */
public final class GiraphGranulaPlatform extends GiraphPlatform implements GranulaAwarePlatform {

	private static final Logger LOG = LogManager.getLogger();

	@Override
	public void preBenchmark(Benchmark benchmark, Path path) {
		PlatformLogger.stopCoreLogging();
		LOG.info(String.format("Logging path at: %s", path.resolve("platform").resolve("driver.logs")));
		PlatformLogger.startPlatformLogging(path.resolve("platform").resolve("driver.logs"));
	}

	@Override
	public void postBenchmark(Benchmark benchmark, Path path) {
		PlatformLogger.collectYarnLogs(path);
		PlatformLogger.stopPlatformLogging();
		PlatformLogger.startCoreLogging();
	}

	@Override
	public JobModel getJobModel() {
		return new JobModel(new Giraph());
	}


	@Override
	public void enrichMetrics(BenchmarkResult benchmarkResult, Path arcDirectory) {
		try {
			PlatformArchive platformArchive = PlatformArchive.readArchive(arcDirectory);
			JSONObject processGraph = platformArchive.operation("ProcessGraph");
			Integer procTime = Integer.parseInt(platformArchive.info(processGraph, "Duration"));
			BenchmarkMetrics metrics = benchmarkResult.getMetrics();
			metrics.setProcessingTime(procTime);
		} catch(Exception e) {
			LOG.error("Failed to enrich metrics.");
		}
	}

}
