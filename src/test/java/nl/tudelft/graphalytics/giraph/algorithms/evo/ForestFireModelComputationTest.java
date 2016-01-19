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
package nl.tudelft.graphalytics.giraph.algorithms.evo;

import nl.tudelft.graphalytics.domain.algorithms.ForestFireModelParameters;
import nl.tudelft.graphalytics.giraph.GiraphTestGraphLoader;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.algorithms.evo.ForestFireModelValidationTest;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Test case for the forest fire model implementation on Giraph. Due to the randomized nature of this algorithm, this
 * test cases focuses on verifying properties that must hold, regardless of random choices made in the algorithm.
 * This includes:
 * <ul>
 * <li>Vertices in the original graph still exist and did not gain or lose any edges.</li>
 * <li>The appropriate number of new vertices were created.</li>
 * <li>Newly created vertices only have outgoing edges to older vertices.</li>
 * <li>All "burnt" vertices are reachable within the maximum number of iterations, via other "burnt" vertices.</li>
 * </ul>
 *
 * @author Tim Hegeman
 */
public class ForestFireModelComputationTest extends ForestFireModelValidationTest {

	@Override
	public GraphStructure executeDirectedForestFireModel(GraphStructure graph, ForestFireModelParameters parameters)
			throws Exception {
		return performTest(DirectedForestFireModelComputation.class, graph, parameters);
	}

	@Override
	public GraphStructure executeUndirectedForestFireModel(GraphStructure graph, ForestFireModelParameters parameters)
			throws Exception {
		return performTest(UndirectedForestFireModelComputation.class, graph, parameters);
	}

	private GraphStructure performTest(Class<? extends Computation> computationClass, GraphStructure graph,
			ForestFireModelParameters parameters) throws Exception {
		GiraphConfiguration configuration = new GiraphConfiguration();
		configuration.setComputationClass(computationClass);
		configuration.setWorkerContextClass(ForestFireModelWorkerContext.class);
		ForestFireModelConfiguration.AVAILABLE_VERTEX_ID.set(configuration, parameters.getMaxId() + 1);
		ForestFireModelConfiguration.BACKWARD_PROBABILITY.set(configuration, parameters.getRRatio());
		ForestFireModelConfiguration.FORWARD_PROBABILITY.set(configuration, parameters.getPRatio());
		ForestFireModelConfiguration.MAX_ITERATIONS.set(configuration, parameters.getMaxIterations());
		ForestFireModelConfiguration.NEW_VERTICES.set(configuration, parameters.getNumNewVertices());

		TestGraph<LongWritable, ForestFireModelData, NullWritable> inputGraph =
				GiraphTestGraphLoader.createGraph(configuration, graph, new ForestFireModelData(), NullWritable.get());

		TestGraph<LongWritable, ForestFireModelData, NullWritable> result =
				InternalVertexRunner.runWithInMemoryOutput(configuration, inputGraph);

		Map<Long, Set<Long>> resultEdges = new HashMap<>();
		for (Map.Entry<LongWritable, Vertex<LongWritable, ForestFireModelData, NullWritable>> vertexEntry :
				result.getVertices().entrySet()) {
			resultEdges.put(vertexEntry.getKey().get(), new HashSet<Long>());
			for (Edge<LongWritable, NullWritable> edge : vertexEntry.getValue().getEdges()) {
				resultEdges.get(vertexEntry.getKey().get()).add(edge.getTargetVertexId().get());
			}
		}
		return new GraphStructure(resultEdges);
	}

}
