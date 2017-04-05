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
package science.atlarge.graphalytics.giraph.algorithms.ffm;

import science.atlarge.graphalytics.domain.graph.FormattedGraph;
import science.atlarge.graphalytics.domain.algorithms.ForestFireModelParameters;
import science.atlarge.graphalytics.giraph.GiraphJob;
import science.atlarge.graphalytics.giraph.io.DirectedLongNullTextEdgeInputFormat;
import science.atlarge.graphalytics.giraph.io.UndirectedLongNullTextEdgeInputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;

import static science.atlarge.graphalytics.giraph.algorithms.ffm.ForestFireModelConfiguration.*;

/**
 * Job configuration of the forest fire model implementation for Giraph.
 *
 * @author Tim Hegeman
 */
public class ForestFireModelJob extends GiraphJob {

	private ForestFireModelParameters parameters;
	private FormattedGraph formattedGraph;

	/**
	 * Constructs a forest fire model job with a EVOParameters object containing
	 * graph-specific parameters, and a graph format specification
	 *
	 * @param parameters  the graph-specific FFM parameters
	 * @param formattedGraph the graph specification
	 */
	public ForestFireModelJob(Object parameters, FormattedGraph formattedGraph) {
		assert (parameters instanceof ForestFireModelParameters);
		this.parameters = (ForestFireModelParameters)parameters;
		this.formattedGraph = formattedGraph;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends Computation> getComputationClass() {
		return (formattedGraph.isDirected() ?
				DirectedForestFireModelComputation.class :
				UndirectedForestFireModelComputation.class);
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends VertexInputFormat> getVertexInputFormatClass() {
		return ForestFireModelVertexInputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends VertexOutputFormat> getVertexOutputFormatClass() {
		return AdjacencyListWithoutValuesVertexOutputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends EdgeInputFormat> getEdgeInputFormatClass() {
		return formattedGraph.isDirected() ?
				DirectedLongNullTextEdgeInputFormat.class :
				UndirectedLongNullTextEdgeInputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Class<? extends EdgeOutputFormat> getEdgeOutputFormatClass() {
		return null;
	}

	@Override
	protected void configure(GiraphConfiguration config) {
		NEW_VERTICES.set(config, parameters.getNumNewVertices());
		AVAILABLE_VERTEX_ID.set(config, parameters.getMaxId() + 1);
		MAX_ITERATIONS.set(config, parameters.getMaxIterations());
		FORWARD_PROBABILITY.set(config, parameters.getPRatio());
		BACKWARD_PROBABILITY.set(config, parameters.getRRatio());

		config.setWorkerContextClass(ForestFireModelWorkerContext.class);
	}

}
