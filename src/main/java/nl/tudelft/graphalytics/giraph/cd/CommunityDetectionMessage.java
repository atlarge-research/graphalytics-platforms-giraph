package nl.tudelft.graphalytics.giraph.cd;

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
