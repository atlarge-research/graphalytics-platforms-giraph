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
package science.atlarge.graphalytics.giraph.algorithms.lcc;

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
