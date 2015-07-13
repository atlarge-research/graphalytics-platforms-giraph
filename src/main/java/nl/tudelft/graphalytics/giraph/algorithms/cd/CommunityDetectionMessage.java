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
 * Message type used for communication in the community detection algorithm.
 *
 * @author Tim Hegeman
 */
public class CommunityDetectionMessage implements Writable {

	private LongWritable sourceId;
	private CommunityDetectionLabel label;

	public CommunityDetectionMessage() {
		this(new LongWritable(), new CommunityDetectionLabel());
	}

	public CommunityDetectionMessage(LongWritable sourceId) {
		this(sourceId, new CommunityDetectionLabel());
	}

	public CommunityDetectionMessage(LongWritable sourceId, CommunityDetectionLabel label) {
		this.sourceId = sourceId;
		this.label = label;
	}

	public LongWritable getSourceId() {
		return sourceId;
	}

	public void setSourceId(LongWritable sourceId) {
		this.sourceId = sourceId;
	}

	public CommunityDetectionLabel getLabel() {
		return label;
	}

	public void setLabel(CommunityDetectionLabel label) {
		this.label = label;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		sourceId.write(out);
		label.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sourceId.readFields(in);
		label.readFields(in);
	}

}
