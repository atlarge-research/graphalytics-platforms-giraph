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

package nl.tudelft.granula.modeller.platform;

import nl.tudelft.grade10.Grade10ModelParser;
import nl.tudelft.granula.modeller.Type;
import nl.tudelft.granula.modeller.job.Job;
import nl.tudelft.granula.modeller.job.Overview;
import nl.tudelft.granula.modeller.platform.info.BasicInfo;
import nl.tudelft.granula.modeller.platform.info.Source;
import nl.tudelft.granula.modeller.platform.operation.*;
import nl.tudelft.granula.modeller.platform.operation.GiraphJob;
import nl.tudelft.granula.modeller.rule.derivation.DerivationRule;
import nl.tudelft.granula.modeller.rule.extraction.GiraphExtractionRule;
import nl.tudelft.granula.modeller.rule.filling.UniqueOperationFilling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Giraph extends PlatformModel {

    public Giraph() {
        super();
        addOperationModel(new GiraphJob());
        addOperationModel(new WorkerPreCompute());
        addOperationModel(new WorkerPostCompute());
        addOperationModel(new WorkerLocalSuperstep());
        addOperationModel(new GiraphSetup());
        addOperationModel(new GiraphExecute());
        addOperationModel(new WorkerCompute());
        addOperationModel(new WorkerPreApplication());
        addOperationModel(new WorkerPostApplication());
        addOperationModel(new BspSuperstep());
        addOperationModel(new GiraphPrepare());
        addOperationModel(new GiraphPostpare());
        addOperationModel(new WorkerThreadParallelCompute());
        addOperationModel(new WorkerThreadVertexCompute());
    }

    public void loadRules() {

        addExtraction(new GiraphExtractionRule(1));
        addFillingRule(new UniqueOperationFilling(2, Type.Giraph, Type.Job));
        addFillingRule(new UniqueOperationFilling(2, Type.Giraph, Type.Execute));
        addFillingRule(new UniqueOperationFilling(2, Type.Giraph, Type.Prepare));
        addFillingRule(new UniqueOperationFilling(2, Type.Giraph, Type.Postpare));
        addInfoDerivation(new JobNameDerivationRule(4));
        addInfoDerivation(new JobInfoRule(20));
        addInfoDerivation(new Grade10ModelParser(21));
    }


    protected class JobInfoRule extends DerivationRule {

        public JobInfoRule(int level) {
            super(level);
        }

        @Override
        public boolean execute() {

            Platform platform = (Platform) entity;
            platform.setName("A Giraph job");
            platform.setType("Giraph");

            Job job = platform.getJob();
            Overview overview = job.getOverview();

            overview.setDescription("This is a Giraph job.");

            Operation jobOper = platform.findOperation(Type.Giraph, Type.Job);
            jobOper.parentId = null;
            platform.addRoot(jobOper.getUuid());

            try {
                Operation processGraph = platform.findOperation(Type.Giraph, Type.Execute);
                long processingTime = Long.parseLong(processGraph.getInfo("Duration").getValue());

                Map<String, Long> breakDown = new HashMap<>();
                breakDown.put("ProcessingTime", processingTime);
//                breakDown.put("IOTime", loadTime);
//                breakDown.put("Overhead", otherTime);
                overview.setBreakDown(breakDown);
            } catch (Exception e) {
                System.out.println(String.format("JobInfoRule encounter %s exception when calculating breakdown.", e.toString()));
            }
            return true;
        }
    }


    protected class JobNameDerivationRule extends DerivationRule {

        public JobNameDerivationRule(int level) {
            super(level);
        }

        @Override
        public boolean execute() {

            Platform platform = (Platform) entity;
            platform.setName("A Giraph job");
            platform.setType("Giraph");
            return true;
        }
    }


}
