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
package nl.tudelft.graphalytics.giraph.io;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Input format for edge-based directed graphs. Inspired by IntNullTextEdgeInputFormat
 * provided by Giraph.
 *
 * @author Tim Hegeman
 */
public class DirectedLongDoubleTextEdgeInputFormat extends TextEdgeInputFormat<LongWritable, DoubleWritable> {

	private static final Pattern SEPARATOR = Pattern.compile(" ");

	@Override
	public EdgeReader<LongWritable, DoubleWritable> createEdgeReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new LongDoubleEdgeReader();
	}

	private class LongDoubleEdgeReader extends TextEdgeReaderFromEachLineProcessed<Triplet<Long, Long, Double>> {

		@Override
		protected Triplet<Long, Long, Double> preprocessLine(Text line) throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			long source = Long.parseLong(tokens[0]);
			long destination = Long.parseLong(tokens[1]);
			double value = Double.parseDouble(tokens[2]);
			return new Triplet<>(source, destination, value);
		}

		@Override
		protected LongWritable getTargetVertexId(Triplet<Long, Long, Double> line)
				throws IOException {
			return new LongWritable(line.getSecond());
		}

		@Override
		protected LongWritable getSourceVertexId(Triplet<Long, Long, Double> line)
				throws IOException {
			return new LongWritable(line.getFirst());
		}

		@Override
		protected DoubleWritable getValue(Triplet<Long, Long, Double> line) throws IOException {
			return new DoubleWritable(line.getThird());
		}

	}

}
