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
package nl.tudelft.graphalytics.giraph.algorithms.stats;

import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Computation for the local clustering coefficient algorithm on Giraph for undirected graphs.
 *
 * @author Tim Hegeman
 */
public class UndirectedLocalClusteringCoefficientComputation extends
		BasicComputation<LongWritable, DoubleWritable, NullWritable, LocalClusteringCoefficientMessage> {

	private LocalClusteringCoefficientMessage msgObject = new LocalClusteringCoefficientMessage();
	private LongSet neighbours = new LongOpenHashSet();
	private LongToLongWritableIterator longWritableIterator = new LongToLongWritableIterator();
	private LongWritable destinationId = new LongWritable();

	@Override
	public void compute(Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
			Iterable<LocalClusteringCoefficientMessage> messages) throws IOException {
		if (getSuperstep() == 0) {
			// First superstep: create a set of neighbours, for each pair ask if they are connected
			collectNeighbourSet(vertex.getEdges());
			sendConnectionInquiries(vertex.getId().get());
		} else if (getSuperstep() == 1) {
			// Second superstep: for each inquiry reply iff the requested edge exists
			sendConnectionReplies(vertex.getEdges(), messages);
		} else if (getSuperstep() == 2) {
			// Third superstep: compute the ratio of responses to requests
			double lcc = computeLCC(Iterables.size(vertex.getEdges()), messages);
			vertex.getValue().set(lcc);
			vertex.voteToHalt();
		}
	}

	private void collectNeighbourSet(Iterable<Edge<LongWritable, NullWritable>> edges) {
		neighbours.clear();

		// Add all edges to the neighbours set
		for (Edge<LongWritable, NullWritable> edge : edges) {
			neighbours.add(edge.getTargetVertexId().get());
		}
	}

	private void sendConnectionInquiries(long sourceVertexId) {
		// No messages to be sent if there is at most one neighbour
		if (neighbours.size() <= 1) {
			return;
		}

		// Send out inquiries in an all-pair fashion
		msgObject.setSource(sourceVertexId);
		msgObject.setEdgeList(neighbours.toLongArray());
		longWritableIterator.reset(neighbours);
		sendMessageToMultipleEdges(longWritableIterator, msgObject);
	}

	private void sendConnectionReplies(Iterable<Edge<LongWritable, NullWritable>> edges,
			Iterable<LocalClusteringCoefficientMessage> inquiries) {
		// Construct a lookup set for the list of edges
		collectNeighbourSet(edges);
		// Loop through the inquiries, count the number of existing edges, and send replies
		for (LocalClusteringCoefficientMessage msg : inquiries) {
			int matchCount = 0;
			for (long edgeId : msg.getEdgeList()) {
				if (neighbours.contains(edgeId)) {
					matchCount++;
				}
			}
			// Send the reply
			destinationId.set(msg.getSource());
			msgObject.setMatchCount(matchCount);
			sendMessage(destinationId, msgObject);
		}
	}

	private static double computeLCC(long numberOfNeighbours, Iterable<LocalClusteringCoefficientMessage> messages) {
		// Any vertex with less than two neighbours can have no edges between neighbours; LCC = 0
		if (numberOfNeighbours < 2) {
			return 0.0;
		}

		// Count the number of (positive) replies
		long numberOfMatches = 0;
		for (LocalClusteringCoefficientMessage msg : messages) {
			numberOfMatches += msg.getMatchCount();
		}
		// Compute the LCC as the ratio between the number of existing edges and number of possible edges
		return (double)numberOfMatches / numberOfNeighbours / (numberOfNeighbours - 1);
	}
}
