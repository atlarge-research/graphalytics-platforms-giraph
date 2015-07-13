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
package nl.tudelft.graphalytics.giraph.algorithms.bfs;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

import static nl.tudelft.graphalytics.giraph.algorithms.bfs.BreadthFirstSearchConfiguration.SOURCE_VERTEX;

/**
 * Implementation of a simple BFS (SSSP) on an unweighted, directed graph.
 *
 * @author Tim Hegeman
 */
public class BreadthFirstSearchComputation extends BasicComputation<LongWritable, LongWritable, NullWritable, LongWritable> {

	/**
	 * Constant vertex value representing an unvisited vertex
	 */
	private static final long UNVISITED = Long.MAX_VALUE;
	/**
	 * Source vertex ID read at the start of the algorithm execution
	 */
	private long sourceVertexId = -1L;

	@Override
	public void setConf(ImmutableClassesGiraphConfiguration<LongWritable, LongWritable, NullWritable> conf) {
		super.setConf(conf);
		sourceVertexId = SOURCE_VERTEX.get(getConf());
	}

	@Override
	public void compute(Vertex<LongWritable, LongWritable, NullWritable> vertex,
			Iterable<LongWritable> messages) throws IOException {
		long bfsDepth = getSuperstep();

		if (getSuperstep() == 0) {
			// During the first superstep only the source vertex should be active
			if (vertex.getId().get() == sourceVertexId) {
				vertex.getValue().set(bfsDepth);
				sendMessageToAllEdges(vertex, vertex.getValue());
			} else {
				vertex.getValue().set(UNVISITED);
			}
		} else {
			// If this vertex was not yet visited, set the vertex depth and propagate to neighbours
			if (vertex.getValue().get() == UNVISITED) {
				vertex.getValue().set(bfsDepth);
				sendMessageToAllEdges(vertex, vertex.getValue());
			}
		}

		// Always halt so the compute method is only executed for those vertices
		// that have an incoming message
		vertex.voteToHalt();
	}

}
