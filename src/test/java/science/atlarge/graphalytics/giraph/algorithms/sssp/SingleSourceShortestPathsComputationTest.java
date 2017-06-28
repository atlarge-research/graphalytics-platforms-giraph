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
package science.atlarge.graphalytics.giraph.algorithms.sssp;

import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import science.atlarge.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters;
import science.atlarge.graphalytics.giraph.GiraphTestGraphLoader;
import science.atlarge.graphalytics.giraph.GiraphTestGraphLoader.WritableConverter;
import science.atlarge.graphalytics.util.graph.PropertyGraph;
import science.atlarge.graphalytics.validation.algorithms.sssp.SingleSourceShortestPathsOutput;
import science.atlarge.graphalytics.validation.algorithms.sssp.SingleSourceShortestPathsValidationTest;

/**
 * @author Tim Hegeman
 */
public class SingleSourceShortestPathsComputationTest extends SingleSourceShortestPathsValidationTest {

	@Override
	public SingleSourceShortestPathsOutput executeDirectedSingleSourceShortestPaths(
			PropertyGraph<Void, Double> graph,
			SingleSourceShortestPathsParameters parameters) throws Exception {
		return execute(graph, parameters);

	}

	@Override
	public SingleSourceShortestPathsOutput executeUndirectedSingleSourceShortestPaths(
			PropertyGraph<Void, Double> graph,
			SingleSourceShortestPathsParameters parameters) throws Exception {
		return execute(graph, parameters);
	}

	private SingleSourceShortestPathsOutput execute(PropertyGraph<Void, Double> graph,
			SingleSourceShortestPathsParameters parameters) throws Exception {
		GiraphConfiguration configuration = new GiraphConfiguration();
		configuration.setComputationClass(SingleSourceShortestPathComputation.class);
		SingleSourceShortestPathConfiguration.SOURCE_VERTEX.set(configuration, parameters.getSourceVertex());

		TestGraph<LongWritable, DoubleWritable, DoubleWritable> inputGraph =
				GiraphTestGraphLoader.createPropertyGraph(
						configuration,
						graph,
						new GiraphTestGraphLoader.DefaultWritableConverter<>(new DoubleWritable(Double.POSITIVE_INFINITY)),
						new WritableConverter<Double, DoubleWritable>() {
							@Override
							public DoubleWritable convert(Double t) {
								return new DoubleWritable(t);
							}
						});


		TestGraph<LongWritable, DoubleWritable, DoubleWritable> result =
				InternalVertexRunner.runWithInMemoryOutput(configuration, inputGraph);

		Map<Long, Double> pathLengths = new HashMap<>();
		for (Map.Entry<LongWritable, Vertex<LongWritable, DoubleWritable, DoubleWritable>> vertexEntry :
				result.getVertices().entrySet()) {
			pathLengths.put(vertexEntry.getKey().get(), vertexEntry.getValue().getValue().get());
		}

		return new SingleSourceShortestPathsOutput(pathLengths);
	}

}
