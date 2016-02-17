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

import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.hadoop.io.DoubleWritable;

import static nl.tudelft.graphalytics.giraph.algorithms.pr.PageRankConfiguration.DANGLING_NODE_SUM;

/**
 * Worker context for the PageRank algorithm to provide access to the dangling node sum of the last iteration.
 *
 * @author Tim Hegeman
 */
public class PageRankWorkerContext extends DefaultWorkerContext {

	private double lastDanglingNodeSum = 0.0;

	@Override
	public void preSuperstep() {
		super.preSuperstep();
		lastDanglingNodeSum = ((DoubleWritable)getAggregatedValue(DANGLING_NODE_SUM)).get();
	}

	public double getLastDanglingNodeSum() {
		return lastDanglingNodeSum;
	}

}
