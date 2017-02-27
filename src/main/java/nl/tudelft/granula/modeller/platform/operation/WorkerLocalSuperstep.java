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

package nl.tudelft.granula.modeller.platform.operation;

import nl.tudelft.granula.modeller.Type;
import nl.tudelft.granula.modeller.rule.derivation.FilialLongAggregationDerivation;
import nl.tudelft.granula.modeller.rule.derivation.SimpleSummaryDerivation;
import nl.tudelft.granula.modeller.rule.linking.EmptyLinking;
import nl.tudelft.granula.modeller.rule.linking.UniqueParentLinking;
import nl.tudelft.granula.modeller.rule.visual.TableVisualization;

import java.util.Arrays;

public class WorkerLocalSuperstep extends RealtimeOperationModel {

    public WorkerLocalSuperstep() {
        super(Type.Worker, Type.LocalSuperstep);
    }

    public void loadRules() {
        super.loadRules();
        addLinkingRule(new UniqueParentLinking(Type.Giraph, Type.Execute));
        addInfoDerivation(new FilialLongAggregationDerivation(3, "Compute", "Duration", "ComputeTime"));
//        addLinkingRule(new EmptyLinking());

        String summary = "LocalSuperstep.";
        addInfoDerivation(new SimpleSummaryDerivation(11, summary));

        addVisualDerivation(new TableVisualization(1, "Informations", Arrays.asList("ComputeTime")));
    }
}
