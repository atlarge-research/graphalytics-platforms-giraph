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

import static nl.tudelft.graphalytics.giraph.algorithms.pr.PageRankConfiguration.DAMPING_FACTOR;
import static nl.tudelft.graphalytics.giraph.algorithms.pr.PageRankConfiguration.DANGLING_NODE_SUM;
import static nl.tudelft.graphalytics.giraph.algorithms.pr.PageRankConfiguration.NUMBER_OF_ITERATIONS;

import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Implementation of the PageRank algorithm on Giraph.
 *
 * @author Tim Hegeman
 */
public class PageRankComputation extends BasicComputation<LongWritable, DoubleWritable, NullWritable, DoubleWritable> {

	private float dampingFactor;
	private int numberOfIterations;

	@Override
	public void setConf(ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, NullWritable> conf) {
		super.setConf(conf);
		dampingFactor = DAMPING_FACTOR.get(conf);
		numberOfIterations = NUMBER_OF_ITERATIONS.get(conf);
	}

	private DoubleWritable msgObject = new DoubleWritable();

	@Override
	public void compute(Vertex<LongWritable, DoubleWritable, NullWritable> vertex, Iterable<DoubleWritable> messages)
			throws IOException {
		if (getSuperstep() == 0) {
			vertex.getValue().set(1.0 / getTotalNumVertices());
		} else {
			double sum = this.<PageRankWorkerContext>getWorkerContext().getLastDanglingNodeSum() / getTotalNumVertices();
			for (DoubleWritable message : messages) {
				sum += message.get();
			}
			vertex.getValue().set((1.0 - dampingFactor) / getTotalNumVertices() + dampingFactor * sum);
		}

		if (getSuperstep() < numberOfIterations) {
			if (vertex.getNumEdges() == 0) {
				aggregate(DANGLING_NODE_SUM, vertex.getValue());
			} else {
				msgObject.set(vertex.getValue().get() / vertex.getNumEdges());
				sendMessageToAllEdges(vertex, msgObject);
			}
		} else {
			vertex.voteToHalt();
		}
	}

}
