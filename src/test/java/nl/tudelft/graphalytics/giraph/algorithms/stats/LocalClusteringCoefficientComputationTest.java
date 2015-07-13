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
package nl.tudelft.graphalytics.giraph.algorithms.stats;

import nl.tudelft.graphalytics.giraph.GiraphTestGraphLoader;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.stats.LocalClusteringCoefficientOutput;
import nl.tudelft.graphalytics.validation.stats.LocalClusteringCoefficientValidationTest;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static nl.tudelft.graphalytics.giraph.algorithms.stats.LocalClusteringCoefficientMasterComputation.LCC_AGGREGATOR_NAME;

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
		configuration.setMasterComputeClass(LocalClusteringCoefficientMasterComputation.class);
		configuration.setAggregatorWriterClass(InMemoryAggregatorWriter.class);

		InMemoryAggregatorWriter.resetAggregator();

		TestGraph<LongWritable, DoubleWritable, NullWritable> inputGraph =
				GiraphTestGraphLoader.createGraph(configuration, graph, new DoubleWritable(-1), NullWritable.get());

		TestGraph<LongWritable, DoubleWritable, NullWritable> result =
				InternalVertexRunner.runWithInMemoryOutput(configuration, inputGraph);

		Map<Long, Double> localClusteringCoefficients = new HashMap<>();
		for (Map.Entry<LongWritable, Vertex<LongWritable, DoubleWritable, NullWritable>> vertexEntry :
				result.getVertices().entrySet()) {
			localClusteringCoefficients.put(vertexEntry.getKey().get(), vertexEntry.getValue().getValue().get());
		}

		Writable lccAggregator = InMemoryAggregatorWriter.getAggregatedValues().get(LCC_AGGREGATOR_NAME);
		double meanLcc = ((DoubleAverage) lccAggregator).get();

		return new LocalClusteringCoefficientOutput(localClusteringCoefficients, meanLcc);
	}

	@Override
	public LocalClusteringCoefficientOutput executeDirectedLocalClusteringCoefficient(GraphStructure graph) throws Exception {
		return executeLocalClusteringCoefficient(DirectedLocalClusteringCoefficientComputation.class, graph);
	}

	@Override
	public LocalClusteringCoefficientOutput executeUndirectedLocalClusteringCoefficient(GraphStructure graph) throws Exception {
		return executeLocalClusteringCoefficient(UndirectedLocalClusteringCoefficientComputation.class, graph);
	}

	public static class InMemoryAggregatorWriter implements AggregatorWriter {

		private static final Map<String, Writable> aggregatedValues = new HashMap<>();

		public static void resetAggregator() {
			aggregatedValues.clear();
		}

		@Override
		public void initialize(Mapper.Context context, long applicationAttempt) throws IOException {
			// Ignored
		}

		@Override
		public void writeAggregator(Iterable<Map.Entry<String, Writable>> aggregatorMap,
				long superstep) throws IOException {
			if (superstep == AggregatorWriter.LAST_SUPERSTEP) {
				for (Map.Entry<String, Writable> aggregator : aggregatorMap) {
					aggregatedValues.put(aggregator.getKey(), aggregator.getValue());
				}
			}
		}

		@Override
		public void close() throws IOException {
			// Ignored
		}

		public static Map<String, Writable> getAggregatedValues() {
			return aggregatedValues;
		}

		@Override
		public ImmutableClassesGiraphConfiguration getConf() {
			return null;
		}

		@Override
		public void setConf(ImmutableClassesGiraphConfiguration configuration) {
			// Ignored
		}
	}

}
