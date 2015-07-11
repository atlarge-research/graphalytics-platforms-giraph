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
package nl.tudelft.graphalytics.giraph.algorithms.cd;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Value type used in the community detection algorithm to store the label and label score of a vertex.
 *
 * @author Tim Hegeman
 */
public class CommunityDetectionLabel implements Writable {

	private LongWritable label;
	private float labelScore;
	private int numberOfNeighbours;

	public CommunityDetectionLabel() {
		this(new LongWritable(), 1.0f, 0);
	}

	public CommunityDetectionLabel(LongWritable label, float labelScore, int numberOfNeighbours) {
		this.label = label;
		this.labelScore = labelScore;
		this.numberOfNeighbours = numberOfNeighbours;
	}

	public LongWritable getLabel() {
		return label;
	}

	public void setLabel(LongWritable label) {
		this.label = label;
	}

	public float getLabelScore() {
		return labelScore;
	}

	public void setLabelScore(float labelScore) {
		this.labelScore = labelScore;
	}

	public int getNumberOfNeighbours() {
		return numberOfNeighbours;
	}

	public void setNumberOfNeighbours(int numberOfNeighbours) {
		this.numberOfNeighbours = numberOfNeighbours;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		label.write(out);
		out.writeFloat(labelScore);
		out.writeInt(numberOfNeighbours);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		label.readFields(in);
		labelScore = in.readFloat();
		numberOfNeighbours = in.readInt();
	}
}
