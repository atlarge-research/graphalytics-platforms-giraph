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

import static nl.tudelft.graphalytics.giraph.algorithms.cd.CommunityDetectionConfiguration.HOP_ATTENUATION;
import static nl.tudelft.graphalytics.giraph.algorithms.cd.CommunityDetectionConfiguration.MAX_ITERATIONS;
import static nl.tudelft.graphalytics.giraph.algorithms.cd.CommunityDetectionConfiguration.NODE_PREFERENCE;
import static nl.tudelft.graphalytics.giraph.algorithms.cd.CommunityDetectionConfiguration.EPSILON;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Community Detection algorithm
 * Credits: mostly Marcin's code refactored
 * Detect Community using algorithm by methods provided in
 * "Towards Real-Time Community Detection in Large Networks by Ian X.Y. Leung,Pan Hui,Pietro Li,and Jon Crowcroft"
 * Changes
 * - refactored private attributes to CommunityDetectionWritable
 * - refactored very long methods
 * - removed unused attributes.
 * Question
 * - why are there two iteration thresholds?
 *
 * @author Wing Ngai
 * @author Tim Hegeman
 */
public class UndirectedCommunityDetectionComputation extends BasicComputation<LongWritable, CommunityDetectionLabel,
        NullWritable, CommunityDetectionMessage> {
    // Load the parameters from the configuration before the compute method to save expensive lookups
	private float nodePreference;
	private float hopAttenuation;
	private int maxIterations;
	
	@Override
	public void setConf(ImmutableClassesGiraphConfiguration<LongWritable, CommunityDetectionLabel, NullWritable> conf) {
		super.setConf(conf);
		nodePreference = NODE_PREFERENCE.get(getConf());
		hopAttenuation = HOP_ATTENUATION.get(getConf());
		maxIterations = MAX_ITERATIONS.get(getConf());
	}
	
    @Override
    public void compute(Vertex<LongWritable, CommunityDetectionLabel, NullWritable> vertex,
            Iterable<CommunityDetectionMessage> messages) throws IOException {
        // max iteration, a stopping condition for data-sets which do not converge
        if (this.getSuperstep() >= maxIterations) {
            determineLabel(vertex, messages);
            vertex.voteToHalt();
            return;
        }

        if (this.getSuperstep() == 0) {
            int edgeCount = 0;
            for (Edge<LongWritable, NullWritable> ignored : vertex.getEdges()) {
                edgeCount++;
            }

            // initialize algorithm, set label as the vertex id, set label score as 1.0
            vertex.getValue().setLabel(vertex.getId().get());
            vertex.getValue().setLabelScore(1.0f);
            vertex.getValue().setNumberOfNeighbours(edgeCount);

            // send initial label to all neighbors
            propagateLabel(vertex);
        }
        else {
            // label assign
            determineLabel(vertex, messages);
            propagateLabel(vertex);
        }
    }

    private CommunityDetectionMessage msgObject = new CommunityDetectionMessage();

    /**
     * Propagate label information to neighbors
     */
    private void propagateLabel(Vertex<LongWritable, CommunityDetectionLabel, NullWritable> vertex) {
        msgObject.setSourceId(vertex.getId());
        msgObject.setLabel(vertex.getValue());
        sendMessageToAllEdges(vertex, msgObject);
    }

    private Long2ObjectMap<CommunityDetectionLabelStatistics> labelStatistics = new Long2ObjectOpenHashMap<>();

    /**
     * Chooses new label AND updates label score
     * - chose new label based on SUM of Label_score(sum all scores of label X) x f(i')^m, where m is number of edges (ignore edge weight == 1) -> EQ 2
     * - score of a vertex new label is a maximal score from all existing scores for that particular label MINUS delta (specified as input parameter) -> EQ 3
     */
    private void determineLabel(Vertex<LongWritable, CommunityDetectionLabel, NullWritable> vertex,
            Iterable<CommunityDetectionMessage> messages) {
        // Compute for each incoming label the aggregate and maximum scores
        labelStatistics.clear();
        for (CommunityDetectionMessage message : messages) {
            long label = message.getLabel().getLabel();
            if (!labelStatistics.containsKey(label)) {
                labelStatistics.put(label, new CommunityDetectionLabelStatistics(label));
            }

            labelStatistics.get(label).addLabel(message.getLabel(), nodePreference);
        }

        // Find the label with the highest aggregate score
        float highestScore = Float.MIN_VALUE;
        CommunityDetectionLabelStatistics winningLabel = null;
        for (Long2ObjectMap.Entry<CommunityDetectionLabelStatistics> singleLabel : labelStatistics.long2ObjectEntrySet()) {
            if (singleLabel.getValue().getAggScore() > highestScore + EPSILON ||
                    (Math.abs(singleLabel.getValue().getAggScore() - highestScore) <= EPSILON &&
                            singleLabel.getLongKey() > winningLabel.getLabel())) {
                highestScore = singleLabel.getValue().getAggScore();
                winningLabel = singleLabel.getValue();
            }
        }

        // Update the label of this vertex
        if (vertex.getValue().getLabel() == winningLabel.getLabel()) {
            vertex.getValue().setLabelScore(winningLabel.getMaxScore());
        } else {
            vertex.getValue().setLabelScore(winningLabel.getMaxScore() - hopAttenuation);
            vertex.getValue().setLabel(winningLabel.getLabel());
        }
    }

}