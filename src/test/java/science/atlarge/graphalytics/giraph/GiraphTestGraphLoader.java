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
package science.atlarge.graphalytics.giraph;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import science.atlarge.graphalytics.util.graph.PropertyGraph;
import science.atlarge.graphalytics.validation.GraphStructure;

/**
 * @author Tim Hegeman
 */
public class GiraphTestGraphLoader {

	public static interface WritableConverter<T, TW extends Writable> {
		public TW convert(T t);
	}

	public static class DefaultWritableConverter<TW extends Writable> implements WritableConverter<Void, TW> {
		final TW value;

		public DefaultWritableConverter(TW value) {
			this.value = value;
		}

		@Override
		public TW convert(Void arg) {
			return value;
		}
	}

	public static <V, E, VW extends Writable, EW extends Writable> TestGraph<LongWritable, VW, EW> createPropertyGraph(
			GiraphConfiguration configuration, PropertyGraph<V, E> input,
			WritableConverter<V, VW> vertexConverter, WritableConverter<E, EW> edgeConverter) {
		TestGraph<LongWritable, VW, EW> graph = new TestGraph<>(configuration);

		for (PropertyGraph<V, E>.Vertex v: input.getVertices()) {
			graph.addVertex(
					new LongWritable(v.getId()),
					vertexConverter.convert(v.getValue()));
		}

		for (PropertyGraph<V, E>.Vertex v: input.getVertices()) {
			for (PropertyGraph<V, E>.Edge e: v.getOutgoingEdges()) {
				graph.addEdge(
						new LongWritable(e.getSourceVertex().getId()),
						new LongWritable(e.getDestinationVertex().getId()),
						edgeConverter.convert(e.getValue()));
			}
		}

		return graph;
	}

	public static <V extends Writable, E extends Writable> TestGraph<LongWritable, V, E> createGraph(
			GiraphConfiguration configuration, GraphStructure input, V vertexValue, E edgeValue) {
		return createPropertyGraph(
				configuration,
				input.toPropertyGraph(),
				new DefaultWritableConverter<>(vertexValue),
				new DefaultWritableConverter<>(edgeValue));
	}
}
