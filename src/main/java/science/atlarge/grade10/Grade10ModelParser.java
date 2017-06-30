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
package science.atlarge.grade10;

import science.atlarge.granula.modeller.Type;
import science.atlarge.granula.modeller.job.Job;
import science.atlarge.granula.modeller.job.Overview;
import science.atlarge.granula.modeller.platform.Platform;
import science.atlarge.granula.modeller.platform.operation.Operation;
import science.atlarge.granula.modeller.rule.derivation.DerivationRule;

public class Grade10ModelParser extends DerivationRule {

    public Grade10ModelParser(int level) {
        super(level);
    }

    @Override
    public boolean execute() {

        Platform platform = (Platform) entity;

        try {
            Operation processGraph = platform.findOperation(Type.Giraph, Type.Execute);
            long processingTime = Long.parseLong(processGraph.getInfo("Duration").getValue());

            System.out.println("Processing time of this job:" + processingTime);
        } catch (Exception e) {
            System.out.println(String.format("Failed to Parse Grade10 Model.", e.toString()));
        }
        return true;
    }
}
