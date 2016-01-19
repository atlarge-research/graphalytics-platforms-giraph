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
package nl.tudelft.graphalytics.giraph.algorithms.pr;

import nl.tudelft.graphalytics.domain.algorithms.PageRankParameters;
import nl.tudelft.graphalytics.giraph.GiraphTestGraphLoader;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.algorithms.pr.PageRankOutput;
import nl.tudelft.graphalytics.validation.algorithms.pr.PageRankValidationTest;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Tim Hegeman
 */
public class PageRankComputationTest extends PageRankValidationTest {

	@Override
	public PageRankOutput executeDirectedPageRank(GraphStructure graph, PageRankParameters parameters)
			throws Exception {
		GiraphConfiguration configuration = new GiraphConfiguration();
		configuration.setComputationClass(PageRankComputation.class);
		PageRankConfiguration.DAMPING_FACTOR.set(configuration, parameters.getDampingFactor());
		PageRankConfiguration.NUMBER_OF_ITERATIONS.set(configuration, parameters.getNumberOfIterations());

		TestGraph<LongWritable, DoubleWritable, NullWritable> inputGraph =
				GiraphTestGraphLoader.createGraph(configuration, graph, new DoubleWritable(), NullWritable.get());

		TestGraph<LongWritable, DoubleWritable, NullWritable> result =
				InternalVertexRunner.runWithInMemoryOutput(configuration, inputGraph);

		Map<Long, Double> pageRanks = new HashMap<>();
		for (Map.Entry<LongWritable, Vertex<LongWritable, DoubleWritable, NullWritable>> vertexEntry :
				result.getVertices().entrySet()) {
			pageRanks.put(vertexEntry.getKey().get(), vertexEntry.getValue().getValue().get());
		}

		return new PageRankOutput(pageRanks);
	}

	@Override
	public PageRankOutput executeUndirectedPageRank(GraphStructure graph, PageRankParameters parameters) throws Exception {
		return executeDirectedPageRank(graph, parameters);
	}

}
