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

/**
 *
 *
 * @author Wing Ngai
 * @author Tim Hegeman
 */
public class CommunityDetectionLabelStatistics {
    LongWritable label;
    float aggScore;
    float maxScore;

    public CommunityDetectionLabelStatistics(LongWritable label) {
        this(label, 0.0f, Float.MIN_VALUE);
    }

    public CommunityDetectionLabelStatistics(LongWritable label, float aggScore, float maxScore) {
        this.label = label;
        this.aggScore = aggScore;
        this.maxScore = maxScore;
    }

    public LongWritable getLabel() {
        return label;
    }

    public void setLabel(LongWritable label) {
        this.label = label;
    }

    public float getAggScore() {
        return aggScore;
    }

    public void setAggScore(float aggScore) {
        this.aggScore = aggScore;
    }

    public float getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(float maxScore) {
        this.maxScore = maxScore;
    }

    public void addLabel(CommunityDetectionLabel other, double nodePreference) {
        if (!other.getLabel().equals(label))
            return;
        aggScore += other.getLabelScore() * Math.pow(other.getNumberOfNeighbours(), nodePreference);
        if (other.getLabelScore() > maxScore) {
            maxScore = other.getLabelScore();
        }
    }

    public void addLabel(CommunityDetectionLabel other, double nodePreference, double scaleFactor) {
        if (!other.getLabel().equals(label))
            return;
        aggScore += other.getLabelScore() * Math.pow(other.getNumberOfNeighbours(), nodePreference) * scaleFactor;
        if (other.getLabelScore() > maxScore) {
            maxScore = other.getLabelScore();
        }
    }

    public void addStatistics(CommunityDetectionLabelStatistics other) {
        if (!other.label.equals(label))
            return;
        aggScore += other.aggScore;
        if (other.maxScore > maxScore) {
            maxScore = other.maxScore;
        }
    }

    @Override
    public String toString() {
        return "CommunityDetectionLabelStatistics{" +
                "label='" + label + '\'' +
                ", aggScore=" + aggScore +
                ", maxScore=" + maxScore +
                '}';
    }
}
