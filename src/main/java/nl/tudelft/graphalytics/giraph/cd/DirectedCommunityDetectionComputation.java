/**
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
package nl.tudelft.graphalytics.giraph.cd;

import static nl.tudelft.graphalytics.giraph.cd.CommunityDetectionConfiguration.HOP_ATTENUATION;
import static nl.tudelft.graphalytics.giraph.cd.CommunityDetectionConfiguration.MAX_ITERATIONS;
import static nl.tudelft.graphalytics.giraph.cd.CommunityDetectionConfiguration.NODE_PREFERENCE;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.*;

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
 * Note: Value on edge is true iff the edge is bidirectional. These edges have double weight in the label selection
 * process.
 *
 * @author Wing Ngai
 * @author Tim Hegeman
 */
public class DirectedCommunityDetectionComputation extends BasicComputation<LongWritable, CommunityDetectionLabel,
		BooleanWritable, CommunityDetectionMessage> {
    // Load the parameters from the configuration before the compute method to save expensive lookups
	private float nodePreference;
	private float hopAttenuation;
	private int maxIterations;
	
	@Override
	public void setConf(ImmutableClassesGiraphConfiguration<LongWritable, CommunityDetectionLabel,
            BooleanWritable> conf) {
		super.setConf(conf);
		nodePreference = NODE_PREFERENCE.get(getConf());
		hopAttenuation = HOP_ATTENUATION.get(getConf());
		maxIterations = MAX_ITERATIONS.get(getConf());
	}

	private static BooleanWritable UNIDIRECTIONAL_EDGE = new BooleanWritable(false);
	private static BooleanWritable BIDIRECTIONAL_EDGE = new BooleanWritable(true);
	private CommunityDetectionMessage msgObject = new CommunityDetectionMessage();
	
    @Override
    public void compute(Vertex<LongWritable, CommunityDetectionLabel, BooleanWritable> vertex,
			Iterable<CommunityDetectionMessage> messages) throws IOException {
        // max iteration, a stopping condition for data-sets which do not converge
        if (this.getSuperstep() > maxIterations+2) {
            vertex.voteToHalt();
            return;
        }

        // send vertex id to outgoing neigbhours, so that all vertices know their incoming edges.
        if (getSuperstep() == 0) {
            msgObject.setSourceId(vertex.getId());
			sendMessageToAllEdges(vertex, msgObject);
        }
        // add incoming edges
        else if (getSuperstep() == 1) {
			// Construct a set of existing edges
            Set<LongWritable> edges = new HashSet<>();
            for (Edge<LongWritable, BooleanWritable> edge : vertex.getEdges()) {
                edges.add(edge.getTargetVertexId());
            }
			// Read incoming messages and add edges/update edge values where appropriate
            for (CommunityDetectionMessage message : messages) {
                if(!edges.contains(message.getSourceId())) {
					vertex.addEdge(EdgeFactory.create(message.getSourceId(), UNIDIRECTIONAL_EDGE));
                    edges.add(message.getSourceId());
                } else {
	                vertex.setEdgeValue(message.getSourceId(), BIDIRECTIONAL_EDGE);
                }
            }
			// initialize algorithm, set label as the vertex id, set label score as 1.0
			vertex.getValue().setLabel(vertex.getId());
            vertex.getValue().setLabelScore(1.0f);
            vertex.getValue().setNumberOfNeighbours(edges.size());

			// send initial label to all neighbors
			propagateLabel(vertex);
        } else {
            // label assign
            determineLabel(vertex, messages);
            propagateLabel(vertex);
        }
    }

    /**
     * Propagate label information to neighbors
     */
    private void propagateLabel(Vertex<LongWritable, CommunityDetectionLabel, BooleanWritable> vertex) {
        msgObject.setSourceId(vertex.getId());
        msgObject.setLabel(vertex.getValue());
        sendMessageToAllEdges(vertex, msgObject);
    }

    /**
     * Chooses new label AND updates label score
     * - chose new label based on SUM of Label_score(sum all scores of label X) x f(i')^m, where m is number of edges (ignore edge weight == 1) -> EQ 2
     * - score of a vertex new label is a maximal score from all existing scores for that particular label MINUS delta (specified as input parameter) -> EQ 3
     */
    private void determineLabel(Vertex<LongWritable, CommunityDetectionLabel, BooleanWritable> vertex,
            Iterable<CommunityDetectionMessage> messages) {
        // Compute for each incoming label the aggregate and maximum scores
        Map<LongWritable, CommunityDetectionLabelStatistics> labelStatistics = new HashMap<>();
        for (CommunityDetectionMessage message : messages) {
            LongWritable label = message.getLabel().getLabel();
            if (!labelStatistics.containsKey(label)) {
                LongWritable cloneLabel = new LongWritable(label.get());
                labelStatistics.put(cloneLabel, new CommunityDetectionLabelStatistics(cloneLabel));
            }

            if (vertex.getEdgeValue(message.getSourceId()).get()) {
                labelStatistics.get(label).addLabel(message.getLabel(), nodePreference, 2.0f);
            } else {
                labelStatistics.get(label).addLabel(message.getLabel(), nodePreference);
            }
        }

        // Find the label with the highest aggregate score
        float highestScore = Float.MIN_VALUE;
        CommunityDetectionLabelStatistics winningLabel = null;
        for (Map.Entry<LongWritable, CommunityDetectionLabelStatistics> singleLabel : labelStatistics.entrySet()) {
            if (singleLabel.getValue().getAggScore() > highestScore) {
                highestScore = singleLabel.getValue().getAggScore();
                winningLabel = singleLabel.getValue();
            }
        }

        // Update the label of this vertex
        if (vertex.getValue().getLabel().equals(winningLabel.getLabel())) {
            vertex.getValue().setLabelScore(winningLabel.getMaxScore());
        } else {
            vertex.getValue().setLabelScore(winningLabel.getMaxScore() - hopAttenuation);
            vertex.getValue().setLabel(winningLabel.getLabel());
        }
    }

}