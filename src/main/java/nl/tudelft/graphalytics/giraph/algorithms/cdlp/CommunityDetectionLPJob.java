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

import nl.tudelft.graphalytics.domain.graph.Graph;
import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import nl.tudelft.graphalytics.giraph.GiraphJob;
import nl.tudelft.graphalytics.giraph.io.UndirectedLongNullTextEdgeInputFormat;
import org.apache.giraph.comm.messages.MessageEncodeAndStoreType;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;

import static nl.tudelft.graphalytics.giraph.algorithms.cdlp.CommunityDetectionLPConfiguration.*;
import static org.apache.giraph.conf.GiraphConstants.MESSAGE_ENCODE_AND_STORE_TYPE;

/**
 * The job configuration of the community detection implementation for Giraph.
 *
 * @author Tim Hegeman
 */
public class CommunityDetectionLPJob extends GiraphJob {

	private CommunityDetectionLPParameters parameters;
	private Graph graph;

	/**
	 * Constructs a community detection job with a CDParameters object containing
	 * graph-specific parameters, and a graph format specification
	 *
	 * @param parameters  the graph-specific CDLP parameters
	 * @param graph the graph format specification
	 */
	public CommunityDetectionLPJob(Object parameters, Graph graph) {
		assert parameters instanceof CommunityDetectionLPParameters;
		this.parameters = (CommunityDetectionLPParameters)parameters;
		this.graph = graph;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends Computation> getComputationClass() {
		return graph.isDirected() ?
				DirectedCommunityDetectionLPComputation.class :
				UndirectedCommunityDetectionLPComputation.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends VertexInputFormat> getVertexInputFormatClass() {
		return graph.isDirected() ?
				CommunityDetectionLPVertexInputFormat.Directed.class :
				CommunityDetectionLPVertexInputFormat.Undirected.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends VertexOutputFormat> getVertexOutputFormatClass() {
		return IdWithValueTextOutputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends EdgeInputFormat> getEdgeInputFormatClass() {
		return graph.isDirected() ?
				DirectedCommunityDetectionLPEdgeInputFormat.class :
				UndirectedLongNullTextEdgeInputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends EdgeOutputFormat> getEdgeOutputFormatClass() {
		return null;
	}

	@Override
	protected void configure(GiraphConfiguration config) {
		MAX_ITERATIONS.set(config, parameters.getMaxIterations());
		// Set the message store type to optimize for one-to-many messages (i.e. broadcasts as used in CC)
		MESSAGE_ENCODE_AND_STORE_TYPE.set(config, MessageEncodeAndStoreType.EXTRACT_BYTEARRAY_PER_PARTITION);
	}

}
