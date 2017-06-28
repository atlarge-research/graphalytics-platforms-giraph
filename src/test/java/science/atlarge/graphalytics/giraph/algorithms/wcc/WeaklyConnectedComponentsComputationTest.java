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
package science.atlarge.graphalytics.giraph.algorithms.wcc;

import science.atlarge.graphalytics.giraph.GiraphTestGraphLoader;
import science.atlarge.graphalytics.validation.GraphStructure;
import science.atlarge.graphalytics.validation.algorithms.wcc.WeaklyConnectedComponentsOutput;
import science.atlarge.graphalytics.validation.algorithms.wcc.WeaklyConnectedComponentsValidationTest;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Tim Hegeman
 */
public class WeaklyConnectedComponentsComputationTest extends WeaklyConnectedComponentsValidationTest {

	private static WeaklyConnectedComponentsOutput executeConnectedComponents(Class<? extends Computation> computationClass,
                                                                              GraphStructure graph) throws Exception {
		GiraphConfiguration configuration = new GiraphConfiguration();
		configuration.setComputationClass(computationClass);

		TestGraph<LongWritable, LongWritable, NullWritable> inputGraph =
				GiraphTestGraphLoader.createGraph(configuration, graph, new LongWritable(-1), NullWritable.get());

		TestGraph<LongWritable, LongWritable, NullWritable> result =
				InternalVertexRunner.runWithInMemoryOutput(configuration, inputGraph);

		Map<Long, Long> pathLengths = new HashMap<>();
		for (Map.Entry<LongWritable, Vertex<LongWritable, LongWritable, NullWritable>> vertexEntry :
				result.getVertices().entrySet()) {
			pathLengths.put(vertexEntry.getKey().get(), vertexEntry.getValue().getValue().get());
		}

		return new WeaklyConnectedComponentsOutput(pathLengths);
	}

	@Override
	public WeaklyConnectedComponentsOutput executeDirectedConnectedComponents(GraphStructure graph) throws Exception {
		return executeConnectedComponents(DirectedWeaklyConnectedComponentsComputation.class, graph);
	}

	@Override
	public WeaklyConnectedComponentsOutput executeUndirectedConnectedComponents(GraphStructure graph) throws Exception {
		return executeConnectedComponents(UndirectedWeaklyConnectedComponentsComputation.class, graph);
	}

}
