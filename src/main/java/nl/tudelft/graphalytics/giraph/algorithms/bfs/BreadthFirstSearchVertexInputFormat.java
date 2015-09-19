package nl.tudelft.graphalytics.giraph.algorithms.bfs;

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Input format for vertices for the breadth first search algorithm.
 *
 * @author Tim Hegeman
 */
public class BreadthFirstSearchVertexInputFormat extends TextVertexValueInputFormat<LongWritable, LongWritable, NullWritable> {

	@Override
	public TextVertexValueReader createVertexValueReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new BreadthFirstSearchVertexReader();
	}

	private class BreadthFirstSearchVertexReader extends TextVertexValueReaderFromEachLine {

		@Override
		protected LongWritable getId(Text line) throws IOException {
			return new LongWritable(Long.parseLong(line.toString()));
		}

		@Override
		protected LongWritable getValue(Text line) throws IOException {
			return new LongWritable(Long.MAX_VALUE);
		}

	}

}
