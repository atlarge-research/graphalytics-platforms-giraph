package nl.tudelft.graphalytics.giraph.algorithms.stats;

import it.unimi.dsi.fastutil.longs.LongCollection;
import it.unimi.dsi.fastutil.longs.LongIterator;
import org.apache.hadoop.io.LongWritable;

import java.util.Iterator;

/**
 * Custom Iterator of LongWritables backed by a fastutil LongIterator.
 *
 * @author Tim Hegeman
 */
public class LongToLongWritableIterator implements Iterator<LongWritable> {

	private LongIterator longIterator;
	private LongWritable longWritable;

	public LongToLongWritableIterator() {
		longIterator = null;
		longWritable = new LongWritable();
	}

	public void reset(LongCollection longs) {
		longIterator = longs.iterator();
	}

	@Override
	public boolean hasNext() {
		return longIterator.hasNext();
	}

	@Override
	public LongWritable next() {
		longWritable.set(longIterator.nextLong());
		return longWritable;
	}

	@Override
	public void remove() {
		// Ignored
	}

}
