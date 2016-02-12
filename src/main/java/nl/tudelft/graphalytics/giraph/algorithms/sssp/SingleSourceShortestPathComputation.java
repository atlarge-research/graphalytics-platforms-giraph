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
package nl.tudelft.graphalytics.giraph.algorithms.sssp;

import static nl.tudelft.graphalytics.giraph.algorithms.bfs.BreadthFirstSearchConfiguration.SOURCE_VERTEX;

import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Implementation of a simple BFS (SSSP) on an unweighted, directed graph.
 *
 * @author Tim Hegeman
 */
public class SingleSourceShortestPathComputation extends BasicComputation<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> {

	/**
	 * Source vertex ID read at the start of the algorithm execution
	 */
	private long sourceVertexId = -1L;

	@Override
	public void setConf(ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, DoubleWritable> conf) {
		super.setConf(conf);
		sourceVertexId = SOURCE_VERTEX.get(getConf());
	}

	private DoubleWritable msg;

	@Override
	public void compute(Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex,
			Iterable<DoubleWritable> messages) throws IOException {

		// New distance of this vertex
		boolean informNeighbors = false;

		// In the first superstep, the source vertex sets its distance to 0.0
		if (getSuperstep() == 0) {
			if(vertex.getId().get() == sourceVertexId) {
				vertex.getValue().set(0.0);
				informNeighbors = true;
			} else {
				vertex.getValue().set(Double.POSITIVE_INFINITY);
			}
		}

		// In subsequent supersteps, vertices need to find the minimum
		// value from the messages sent by their neighbors
		else {
			double minDist = Double.POSITIVE_INFINITY;

			// find minimum
			for (DoubleWritable message: messages) {
				System.out.println(vertex.getId() + " received " + " " + message.get());

				if (message.get() < minDist) {
					minDist = message.get();
				}
			}

			System.out.println(vertex.getId() + " checks " + " " + minDist + " < " + vertex.getValue().get());

			// if smaller, set new distance and update neighbors
			if (minDist < vertex.getValue().get()) {
				vertex.getValue().set(minDist);
				informNeighbors = true;
			}
		}

		// Send messages to neighbors to inform them of new distance
		if (informNeighbors) {
			double dist = vertex.getValue().get();

			for (Edge<LongWritable, DoubleWritable> edge: vertex.getEdges()) {
				LongWritable id = edge.getTargetVertexId();
				double value = edge.getValue().get();

				msg.set(dist + value);
				sendMessage(id, msg);
			}
		}

		// Always halt so the compute method is only executed for those vertices
		// that have an incoming message
		vertex.voteToHalt();
	}
}

