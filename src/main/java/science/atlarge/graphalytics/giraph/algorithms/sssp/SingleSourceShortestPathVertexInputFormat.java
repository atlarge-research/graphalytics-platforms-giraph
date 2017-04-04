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
package science.atlarge.graphalytics.giraph.algorithms.sssp;

import java.io.IOException;

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SingleSourceShortestPathVertexInputFormat extends TextVertexValueInputFormat<LongWritable, DoubleWritable, DoubleWritable> {

	@Override
	public TextVertexValueReader createVertexValueReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new SingleSourceShortestPathVertexReader();
	}

	private class SingleSourceShortestPathVertexReader extends TextVertexValueReaderFromEachLine {

		@Override
		protected LongWritable getId(Text line) throws IOException {
			return new LongWritable(Long.parseLong(line.toString()));
		}

		@Override
		protected DoubleWritable getValue(Text line) throws IOException {
			return new DoubleWritable(Double.POSITIVE_INFINITY);
		}
	}
}
