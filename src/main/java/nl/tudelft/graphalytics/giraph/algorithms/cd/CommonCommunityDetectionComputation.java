package nl.tudelft.graphalytics.giraph.algorithms.cd;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

import static nl.tudelft.graphalytics.giraph.algorithms.cd.CommunityDetectionConfiguration.MAX_ITERATIONS;

/**
 * Common features of the Community Detection algorithm using label propagation, based on Raghavan, et al. "Near linear
 * time algorithm to detect community structures in large-scale networks", Phys. Rev. E 76. Inherited to produce both a
 * directed and undirected implementation.
 *
 * @author Tim Hegeman
 */
public abstract class CommonCommunityDetectionComputation<E extends Writable> extends BasicComputation<LongWritable,
		LongWritable, E, LongWritable> {

	// Load the parameters from the configuration before the compute method to save expensive look-ups
	private int maxIterations;

	@Override
	public void setConf(ImmutableClassesGiraphConfiguration<LongWritable, LongWritable, E> conf) {
		super.setConf(conf);
		maxIterations = MAX_ITERATIONS.get(getConf());
	}

	@Override
	public void compute(Vertex<LongWritable, LongWritable, E> vertex, Iterable<LongWritable> messages)
			throws IOException {
		// max iteration, a stopping condition for data-sets which do not converge
		if (getSuperstep() >= maxIterations + getNumberOfInitialisationSteps()) {
			determineLabel(vertex, messages);
			vertex.voteToHalt();
		} else if (getSuperstep() < getNumberOfInitialisationSteps()) {
			doInitialisationStep(vertex, messages);
			if (getSuperstep() == getNumberOfInitialisationSteps() - 1) {
				propagateLabel(vertex);
			}
		} else {
			determineLabel(vertex, messages);
			propagateLabel(vertex);
		}
	}

	/**
	 * Hook to perform initialisation.
	 */
	protected abstract void doInitialisationStep(Vertex<LongWritable, LongWritable, E> vertex,
			Iterable<LongWritable> messages);

	/**
	 * @return the number of supersteps spent initialising the algorithm
	 */
	protected abstract int getNumberOfInitialisationSteps();

	/**
	 * Propagate the label of a vertex to its neighbours.
	 */
	protected abstract void propagateLabel(Vertex<LongWritable, LongWritable, E> vertex);

	private Long2LongMap labelOccurences = new Long2LongOpenHashMap();

	/**
	 * Chooses new label by finding the most frequent label and picking the lowest id amongst the most frequent.
	 */
	private void determineLabel(Vertex<LongWritable, LongWritable, E> vertex,
			Iterable<LongWritable> incomingLabels) {
		// Compute for each incoming label the aggregate and maximum scores
		labelOccurences.clear();
		labelOccurences.defaultReturnValue(0L);
		for (LongWritable incomingLabel : incomingLabels) {
			long label = incomingLabel.get();
			labelOccurences.put(label, labelOccurences.get(label) + 1);
		}

		// Find the label with the highest frequency score (primary key) and lowest id (secondary key)
		long bestLabel = 0;
		long highestFrequency = 0;
		for (Long2LongMap.Entry labelFrequency : labelOccurences.long2LongEntrySet()) {
			long label = labelFrequency.getLongKey();
			long frequency = labelFrequency.getLongValue();
			if (frequency > highestFrequency || (frequency == highestFrequency && label < bestLabel)) {
				bestLabel = label;
				highestFrequency = frequency;
			}
		}

		// Update the label of this vertex
		vertex.getValue().set(bestLabel);
	}

}
