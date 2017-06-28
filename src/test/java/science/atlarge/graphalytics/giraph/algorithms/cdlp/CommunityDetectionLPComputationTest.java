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
package science.atlarge.graphalytics.giraph.algorithms.cdlp;

import science.atlarge.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import science.atlarge.graphalytics.giraph.GiraphTestGraphLoader;
import science.atlarge.graphalytics.validation.GraphStructure;
import science.atlarge.graphalytics.validation.algorithms.cdlp.CommunityDetectionLPOutput;
import science.atlarge.graphalytics.validation.algorithms.cdlp.CommunityDetectionLPValidationTest;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import java.util.HashMap;
import java.util.Map;

/**
 * Test cases for validating the Giraph community detection implementation using Graphalytics' validation framework.
 *
 * @author Tim Hegeman
 */
public class CommunityDetectionLPComputationTest extends CommunityDetectionLPValidationTest {

	private static GiraphConfiguration configurationFromParameters(Class<? extends Computation> computationClass,
			CommunityDetectionLPParameters parameters) {
		GiraphConfiguration configuration = new GiraphConfiguration();
		configuration.setComputationClass(computationClass);
		CommunityDetectionLPConfiguration.MAX_ITERATIONS.set(configuration, parameters.getMaxIterations());
		return configuration;
	}

	private static <E extends Writable> CommunityDetectionLPOutput outputFromResultGraph(
			TestGraph<LongWritable, LongWritable, E> result) {
		Map<Long, Long> communityIds = new HashMap<>();
		for (Map.Entry<LongWritable, Vertex<LongWritable, LongWritable, E>> vertexEntry :
				result.getVertices().entrySet()) {
			communityIds.put(vertexEntry.getKey().get(), vertexEntry.getValue().getValue().get());
		}

		return new CommunityDetectionLPOutput(communityIds);
	}

	@Override
	public CommunityDetectionLPOutput executeDirectedCommunityDetection(
			GraphStructure graph, CommunityDetectionLPParameters parameters) throws Exception {
		GiraphConfiguration configuration = configurationFromParameters(DirectedCommunityDetectionLPComputation.class,
				parameters);

		TestGraph<LongWritable, LongWritable, BooleanWritable> inputGraph =
				GiraphTestGraphLoader.createGraph(configuration, graph, new LongWritable(),
						new BooleanWritable());

		TestGraph<LongWritable, LongWritable, BooleanWritable> result =
				InternalVertexRunner.runWithInMemoryOutput(configuration, inputGraph);

		return outputFromResultGraph(result);
	}

	@Override
	public CommunityDetectionLPOutput executeUndirectedCommunityDetection(
			GraphStructure graph, CommunityDetectionLPParameters parameters) throws Exception {
		GiraphConfiguration configuration = configurationFromParameters(UndirectedCommunityDetectionLPComputation.class,
				parameters);

		TestGraph<LongWritable, LongWritable, NullWritable> inputGraph =
				GiraphTestGraphLoader.createGraph(configuration, graph, new LongWritable(),
						NullWritable.get());

		TestGraph<LongWritable, LongWritable, NullWritable> result =
				InternalVertexRunner.runWithInMemoryOutput(configuration, inputGraph);

		return outputFromResultGraph(result);
	}
}
