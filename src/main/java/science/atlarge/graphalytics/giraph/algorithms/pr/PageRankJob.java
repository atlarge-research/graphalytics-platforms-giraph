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
package science.atlarge.graphalytics.giraph.algorithms.pr;

import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.domain.algorithms.PageRankParameters;
import science.atlarge.graphalytics.giraph.GiraphJob;
import science.atlarge.graphalytics.giraph.io.DirectedLongNullTextEdgeInputFormat;
import science.atlarge.graphalytics.giraph.io.UndirectedLongNullTextEdgeInputFormat;
import org.apache.giraph.comm.messages.MessageEncodeAndStoreType;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;

import static org.apache.giraph.conf.GiraphConstants.MESSAGE_ENCODE_AND_STORE_TYPE;

/**
 * The job configuration of the PageRank implementation for Giraph.
 *
 * @author Tim Hegeman
 */
public class PageRankJob extends GiraphJob {

	private final PageRankParameters parameters;
	private final Graph graph;

	/**
	 * Constructs a PageRank job with a PageRankParameters object containing graph-specific parameters,
	 * and a graph format specification
	 *
	 * @param parameters  the graph-specific PageRank parameters
	 * @param graph the graph format specification
	 */
	public PageRankJob(Object parameters, Graph graph) {
		assert (parameters instanceof PageRankParameters);
		this.parameters = (PageRankParameters)parameters;
		this.graph = graph;
	}

	@Override
	protected Class<? extends Computation> getComputationClass() {
		return PageRankComputation.class;
	}

	@Override
	protected Class<? extends VertexInputFormat> getVertexInputFormatClass() {
		return PageRankVertexInputFormat.class;
	}

	@Override
	protected Class<? extends VertexOutputFormat> getVertexOutputFormatClass() {
		return IdWithValueTextOutputFormat.class;
	}

	@Override
	protected Class<? extends EdgeInputFormat> getEdgeInputFormatClass() {
		return graph.isDirected() ?
				DirectedLongNullTextEdgeInputFormat.class :
				UndirectedLongNullTextEdgeInputFormat.class;
	}

	@Override
	protected Class<? extends EdgeOutputFormat> getEdgeOutputFormatClass() {
		return null;
	}

	@Override
	protected void configure(GiraphConfiguration config) {
		PageRankConfiguration.DAMPING_FACTOR.set(config, parameters.getDampingFactor());
		PageRankConfiguration.NUMBER_OF_ITERATIONS.set(config, parameters.getNumberOfIterations());
		// Set the message store type to optimize for one-to-many messages (i.e. broadcasts as used in PageRank)
		MESSAGE_ENCODE_AND_STORE_TYPE.set(config, MessageEncodeAndStoreType.EXTRACT_BYTEARRAY_PER_PARTITION);

		config.setMasterComputeClass(PageRankMasterComputation.class);
		config.setWorkerContextClass(PageRankWorkerContext.class);
	}

}
