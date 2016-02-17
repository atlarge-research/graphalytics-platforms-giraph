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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Input format for edge-based directed graphs. Inspired by IntNullTextEdgeInputFormat
 * provided by Giraph.
 *
 * @author Tim Hegeman
 */
public class UndirectedLongDoubleTextEdgeInputFormat extends TextEdgeInputFormat<LongWritable, DoubleWritable> {

	private static final Pattern SEPARATOR = Pattern.compile(" ");

	@Override
	public EdgeReader<LongWritable, DoubleWritable> createEdgeReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new LongDoubleEdgeReader();
	}

	private class LongDoubleEdgeReader extends TextEdgeReader {

		private boolean outputBackwards = true;
		private long first;
		private long second;
		private double value;

		@Override
		public boolean nextEdge() throws IOException, InterruptedException {
			if (!outputBackwards) {
				outputBackwards = true;
				return true;
			}

			if (!getRecordReader().nextKeyValue()) {
				return false;
			}

			String[] tokens = SEPARATOR.split(getRecordReader().getCurrentValue().toString());
			first = Long.parseLong(tokens[0]);
			second = Long.parseLong(tokens[1]);
			value = Double.parseDouble(tokens[2]);
			outputBackwards = false;
			return true;
		}

		@Override
		public LongWritable getCurrentSourceId() throws IOException, InterruptedException {
			return new LongWritable(outputBackwards ? second : first);
		}

		@Override
		public Edge<LongWritable, DoubleWritable> getCurrentEdge() throws IOException, InterruptedException {
			return EdgeFactory.create(new LongWritable(outputBackwards ? first : second), new DoubleWritable(value));
		}

	}

}
