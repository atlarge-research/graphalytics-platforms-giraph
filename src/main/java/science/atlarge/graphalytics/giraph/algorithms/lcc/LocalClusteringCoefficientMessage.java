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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Message class representing the various types of messages sent in the LCC algorithm.
 *
 * @author Tim Hegeman
 */
public class LocalClusteringCoefficientMessage implements Writable {

	private static final int SOURCE_PRESENT = 1;
	private static final int EDGELIST_PRESENT = 2;
	private static final int MATCHCOUNT_PRESENT = 4;

	private int validFields;
	private long source;
	private long[] edgeList;
	private int matchCount;

	public LocalClusteringCoefficientMessage() {
		this(0, 0, null, 0);
	}

	/**
	 * Used for informing neighbours of the existence of an incoming edge
	 * (directed graphs only).
	 *
	 * @param source the source vertex ID.
	 */
	public LocalClusteringCoefficientMessage(long source) {
		this(SOURCE_PRESENT, source, null, 0);
	}

	/**
	 * Used for acknowledging the existence of a number of edges.
	 *
	 * @param matchCount the number of requested edges found.
	 */
	public LocalClusteringCoefficientMessage(int matchCount) {
		this(MATCHCOUNT_PRESENT, 0, null, matchCount);
	}

	/**
	 * Used for requesting information about the existence of edges (between
	 * the recipient of the message and a list of destination edges) while
	 * expecting an answer to be sent back to source.
	 *
	 * @param source   the source vertex ID.
	 * @param edgeList the destination vertex IDs of the edges we wish to
	 *                 know the existence of.
	 */
	public LocalClusteringCoefficientMessage(long source, long[] edgeList) {
		this(SOURCE_PRESENT | EDGELIST_PRESENT, source, edgeList, 0);
	}

	private LocalClusteringCoefficientMessage(int validFields, long source, long[] edgeList, int matchCount) {
		this.validFields = validFields;
		this.source = source;
		this.edgeList = edgeList;
		this.matchCount = matchCount;
	}

	public boolean hasSource() {
		return (validFields & SOURCE_PRESENT) == SOURCE_PRESENT;
	}

	public long getSource() {
		return source;
	}

	public void setSource(long source) {
		this.source = source;
		validFields |= SOURCE_PRESENT;
	}

	public void clearSource() {
		validFields &= ~SOURCE_PRESENT;
	}

	public boolean hasEdgeList() {
		return (validFields & EDGELIST_PRESENT) == EDGELIST_PRESENT;
	}

	public long[] getEdgeList() {
		return edgeList;
	}

	public void setEdgeList(long[] edgeList) {
		this.edgeList = edgeList;
		validFields |= EDGELIST_PRESENT;
	}

	public void clearEdgeList() {
		validFields &= ~EDGELIST_PRESENT;
	}

	public boolean hasMatchCount() {
		return (validFields & MATCHCOUNT_PRESENT) == MATCHCOUNT_PRESENT;
	}

	public int getMatchCount() {
		return matchCount;
	}

	public void setMatchCount(int matchCount) {
		this.matchCount = matchCount;
		validFields |= MATCHCOUNT_PRESENT;
	}

	public void clearMatchCount() {
		validFields &= ~MATCHCOUNT_PRESENT;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChar(validFields);
		if ((validFields & SOURCE_PRESENT) == SOURCE_PRESENT) {
			out.writeLong(source);
		}
		if ((validFields & EDGELIST_PRESENT) == EDGELIST_PRESENT) {
			out.writeInt(edgeList.length);
			for (long edge : edgeList) {
				out.writeLong(edge);
			}
		}
		if ((validFields & MATCHCOUNT_PRESENT) == MATCHCOUNT_PRESENT) {
			out.writeInt(matchCount);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		validFields = in.readChar();
		if ((validFields & SOURCE_PRESENT) == SOURCE_PRESENT) {
			source = in.readLong();
		}
		if ((validFields & EDGELIST_PRESENT) == EDGELIST_PRESENT) {
			edgeList = new long[in.readInt()];
			for (int i = 0; i < edgeList.length; i++) {
				edgeList[i] = in.readLong();
			}
		}
		if ((validFields & MATCHCOUNT_PRESENT) == MATCHCOUNT_PRESENT) {
			matchCount = in.readInt();
		}
	}

}
