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

import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Specialisation of {@link CommonCommunityDetectionLPComputation} for directed graphs. The value of an edge is true iff
 * the edge is bidirectional. These edges have double weight in the label selection process.
 *
 * @author Tim Hegeman
 */
public class DirectedCommunityDetectionLPComputation extends CommonCommunityDetectionLPComputation<BooleanWritable> {

	private static final boolean UNIDIRECTIONAL = false;
	private static final BooleanWritable UNIDIRECTIONAL_EDGE = new BooleanWritable(UNIDIRECTIONAL);
	private static final boolean BIDIRECTIONAL = true;

	@Override
	protected void doInitialisationStep(Vertex<LongWritable, LongWritable, BooleanWritable> vertex,
			Iterable<LongWritable> messages) {
		if (getSuperstep() == 0) {
			// Send vertex id to outgoing neighbours, so that all vertices know their incoming edges.
			sendMessageToAllEdges(vertex, vertex.getId());
		} else {
			// Store incoming messages (vertex ids) in a set
			LongSet messageSet = new LongOpenHashSet();
			for (LongWritable message : messages) {
				messageSet.add(message.get());
			}
			// Update the value of existing edges
			for (MutableEdge<LongWritable, BooleanWritable> edge : vertex.getMutableEdges()) {
				long targetVertexId = edge.getTargetVertexId().get();
				if (messageSet.contains(targetVertexId)) {
					messageSet.remove(targetVertexId);
					edge.getValue().set(BIDIRECTIONAL);
				}
			}
			// Create new unidirectional edges to match incoming edges
			for (LongIterator messageIter = messageSet.iterator(); messageIter.hasNext(); ) {
				long newEdge = messageIter.nextLong();
				vertex.addEdge(EdgeFactory.create(new LongWritable(newEdge), UNIDIRECTIONAL_EDGE));
			}

			// Set the initial label of the vertex
			vertex.getValue().set(vertex.getId().get());
		}
	}

	@Override
	protected int getNumberOfInitialisationSteps() {
		return 2;
	}

	@Override
	protected void propagateLabel(Vertex<LongWritable, LongWritable, BooleanWritable> vertex) {
		LongWritable message = vertex.getValue();
		for (Edge<LongWritable, BooleanWritable> edge : vertex.getEdges()) {
			sendMessage(edge.getTargetVertexId(), message);
			// Send twice on bidirectional edges
			if (edge.getValue().get() == BIDIRECTIONAL) {
				sendMessage(edge.getTargetVertexId(), message);
			}
		}
	}

}