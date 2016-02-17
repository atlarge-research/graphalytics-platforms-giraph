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
package nl.tudelft.graphalytics.giraph.algorithms.cdlp;

import org.apache.giraph.conf.IntConfOption;

/**
 * Configuration constants for community detection on Giraph.
 *
 * @author Tim Hegeman
 */
public final class CommunityDetectionLPConfiguration {

	/**
	 * Configuration key for the maximum number of iterations of the algorithm.
	 */
	public static final String MAX_ITERATIONS_KEY = "graphalytics.cd.max-iterations";
	/**
	 * The maximum number of iterations to run the community detection algorithm for.
	 */
	public static final IntConfOption MAX_ITERATIONS = new IntConfOption(
			MAX_ITERATIONS_KEY, 1, "Maximum number of iterations to run the community detection algorithm for.");

	private CommunityDetectionLPConfiguration() {
	}

}
