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

import static nl.tudelft.graphalytics.giraph.algorithms.sssp.SingleSourceShortestPathConfiguration.SOURCE_VERTEX;
import static org.apache.giraph.conf.GiraphConstants.MESSAGE_ENCODE_AND_STORE_TYPE;

import org.apache.giraph.comm.messages.MessageEncodeAndStoreType;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;

import nl.tudelft.graphalytics.domain.Graph;
import nl.tudelft.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters;
import nl.tudelft.graphalytics.giraph.GiraphJob;
import nl.tudelft.graphalytics.giraph.io.DirectedLongDoubleTextEdgeInputFormat;
import nl.tudelft.graphalytics.giraph.io.UndirectedLongDoubleTextEdgeInputFormat;

/**
 * The job configuration of the single source shortest path implementation for Giraph.
 *
 * @author Tim Hegeman
 */
public class SingleSourceShortestPathJob extends GiraphJob {

	private SingleSourceShortestPathsParameters parameters;
	private Graph graphFormat;

	/**
	 * Constructs a breadth-first-search job with a BFSParameters object containing
	 * graph-specific parameters, and a graph format specification
	 *
	 * @param parameters  the graph-specific BFS parameters
	 * @param graphFormat the graph format specification
	 */
	public SingleSourceShortestPathJob(Object parameters, Graph graphFormat) {
		assert (parameters instanceof SingleSourceShortestPathsParameters);
		this.parameters = (SingleSourceShortestPathsParameters) parameters;
		this.graphFormat = graphFormat;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends Computation> getComputationClass() {
		return SingleSourceShortestPathComputation.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends VertexInputFormat> getVertexInputFormatClass() {
		return SingleSourceShortestPathVertexInputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends VertexOutputFormat> getVertexOutputFormatClass() {
		return IdWithValueTextOutputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends EdgeInputFormat> getEdgeInputFormatClass() {
		return graphFormat.isDirected() ?
				DirectedLongDoubleTextEdgeInputFormat.class :
				UndirectedLongDoubleTextEdgeInputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends EdgeOutputFormat> getEdgeOutputFormatClass() {
		return null;
	}

	@Override
	protected void configure(GiraphConfiguration config) {
		SOURCE_VERTEX.set(config, parameters.getSourceVertex());

		// Set the message store type to optimize for one-to-many messages (i.e. broadcasts as used in BFS)
		MESSAGE_ENCODE_AND_STORE_TYPE.set(config, MessageEncodeAndStoreType.EXTRACT_BYTEARRAY_PER_PARTITION);
	}

}
