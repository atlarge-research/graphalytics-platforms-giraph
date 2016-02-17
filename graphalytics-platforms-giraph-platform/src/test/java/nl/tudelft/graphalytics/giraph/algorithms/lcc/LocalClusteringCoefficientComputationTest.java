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
package nl.tudelft.graphalytics.giraph.algorithms.lcc;

import nl.tudelft.graphalytics.giraph.GiraphTestGraphLoader;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.algorithms.lcc.LocalClusteringCoefficientOutput;
import nl.tudelft.graphalytics.validation.algorithms.lcc.LocalClusteringCoefficientValidationTest;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.HashMap;
import java.util.Map;

/**
 * Test class for computing the local clustering coefficient. Executes the Giraph implementation of the LCC computation
 * on a small graph, and verifies that the output of the computation matches the expected results.
 *
 * @author Tim Hegeman
 */
public class LocalClusteringCoefficientComputationTest extends LocalClusteringCoefficientValidationTest {

	private static LocalClusteringCoefficientOutput executeLocalClusteringCoefficient(
			Class<? extends Computation> computationClass, GraphStructure graph) throws Exception {
		GiraphConfiguration configuration = new GiraphConfiguration();
		configuration.setComputationClass(computationClass);

		TestGraph<LongWritable, DoubleWritable, NullWritable> inputGraph =
				GiraphTestGraphLoader.createGraph(configuration, graph, new DoubleWritable(-1), NullWritable.get());

		TestGraph<LongWritable, DoubleWritable, NullWritable> result =
				InternalVertexRunner.runWithInMemoryOutput(configuration, inputGraph);

		Map<Long, Double> localClusteringCoefficients = new HashMap<>();
		for (Map.Entry<LongWritable, Vertex<LongWritable, DoubleWritable, NullWritable>> vertexEntry :
				result.getVertices().entrySet()) {
			localClusteringCoefficients.put(vertexEntry.getKey().get(), vertexEntry.getValue().getValue().get());
		}
		return new LocalClusteringCoefficientOutput(localClusteringCoefficients);
	}

	@Override
	public LocalClusteringCoefficientOutput executeDirectedLocalClusteringCoefficient(GraphStructure graph) throws Exception {
		return executeLocalClusteringCoefficient(DirectedLocalClusteringCoefficientComputation.class, graph);
	}

	@Override
	public LocalClusteringCoefficientOutput executeUndirectedLocalClusteringCoefficient(GraphStructure graph) throws Exception {
		return executeLocalClusteringCoefficient(UndirectedLocalClusteringCoefficientComputation.class, graph);
	}

}
