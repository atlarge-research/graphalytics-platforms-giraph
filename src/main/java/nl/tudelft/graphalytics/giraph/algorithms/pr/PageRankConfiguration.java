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
package nl.tudelft.graphalytics.giraph.algorithms.pr;

import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.IntConfOption;

/**
 * Configuration constants for PageRank on Giraph.
 *
 * @author Tim Hegeman
 */
public final class PageRankConfiguration {

	/**
	 * Configuration key for the damping factor to use in the PageRank algorithm
	 */
	public static final String DAMPING_FACTOR_KEY = "graphalytics.pr.damping-factor";
	/**
	 * Configuration option for the damping factor to use in the PageRank algorithm
	 */
	public static final FloatConfOption DAMPING_FACTOR = new FloatConfOption(
			DAMPING_FACTOR_KEY, 1.0f, "Damping factor to use in the PageRank algorithm");

	/**
	 * Configuration key for the number of iterations to run the PageRank algorithm for
	 */
	public static final String NUMBER_OF_ITERATIONS_KEY = "graphalytics.pr.num-iterations";
	/**
	 * Configuration option for the number of iterations to run the PageRank algorithm for
	 */
	public static final IntConfOption NUMBER_OF_ITERATIONS = new IntConfOption(
			NUMBER_OF_ITERATIONS_KEY, -1, "Number of iterations to run the PageRank algorithm for");

	private PageRankConfiguration() {
	}

}
