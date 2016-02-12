package nl.tudelft.graphalytics.giraph.algorithms.sssp;

import java.io.IOException;

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
			return new DoubleWritable(Double.MAX_VALUE);
		}
	}
}
