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

import science.atlarge.graphalytics.giraph.io.Triplet;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import science.atlarge.graphalytics.giraph.io.DirectedLongNullTextEdgeInputFormat;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Similar to {@link DirectedLongNullTextEdgeInputFormat
 * DirectedLongNullTextEdgeInputFormat}, except that edges have {@link BooleanWritable} values.
 *
 * @author Tim Hegeman
 */
public class DirectedCommunityDetectionLPEdgeInputFormat extends TextEdgeInputFormat<LongWritable, BooleanWritable> {

	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	@Override
	public EdgeReader<LongWritable, BooleanWritable> createEdgeReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new LongBooleanEdgeReader();
	}

	private class LongBooleanEdgeReader extends TextEdgeReaderFromEachLineProcessed<Triplet<Long, Long, Void>> {

		@Override
		protected Triplet preprocessLine(Text line) throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			long source = Long.parseLong(tokens[0]);
			long destination = Long.parseLong(tokens[1]);
			return new Triplet(source, destination, null);
		}

		@Override
		protected LongWritable getTargetVertexId(Triplet<Long, Long, Void> line)
				throws IOException {
			return new LongWritable(line.getSecond());
		}

		@Override
		protected LongWritable getSourceVertexId(Triplet<Long, Long, Void> line)
				throws IOException {
			return new LongWritable(line.getFirst());
		}

		@Override
		protected BooleanWritable getValue(Triplet<Long, Long, Void> line) throws IOException {
			return new BooleanWritable(false);
		}

	}

}
