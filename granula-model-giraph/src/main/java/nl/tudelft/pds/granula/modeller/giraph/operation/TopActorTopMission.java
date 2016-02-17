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

package nl.tudelft.pds.granula.modeller.giraph.operation;

import nl.tudelft.pds.granula.archiver.entity.info.*;
import nl.tudelft.pds.granula.archiver.entity.operation.Operation;
import nl.tudelft.pds.granula.modeller.giraph.GiraphType;
import nl.tudelft.pds.granula.modeller.model.operation.AbstractOperationModel;
import nl.tudelft.pds.granula.modeller.rule.derivation.BasicSummaryDerivation;
import nl.tudelft.pds.granula.modeller.rule.derivation.DerivationRule;
import nl.tudelft.pds.granula.modeller.rule.derivation.time.DurationDerivation;
import nl.tudelft.pds.granula.modeller.rule.derivation.time.FilialEndTimeDerivation;
import nl.tudelft.pds.granula.modeller.rule.derivation.time.FilialStartTimeDerivation;
import nl.tudelft.pds.granula.modeller.rule.linking.EmptyLinking;
import nl.tudelft.pds.granula.modeller.rule.visual.MainInfoTableVisualization;
import nl.tudelft.pds.granula.modeller.rule.visual.TableVisualization;

import java.util.ArrayList;
import java.util.List;

public class TopActorTopMission extends AbstractOperationModel {

    public TopActorTopMission() {
        super(GiraphType.TopActor, GiraphType.TopMission);
    }

    public void loadRules() {
        super.loadRules();

        addLinkingRule(new EmptyLinking());

//        addInfoDerivation(new FilialStringDerivation(3, OpenGType.Deployment, "ApplicationName"));
//        addInfoDerivation(new FilialStringDerivation(3, OpenGType.Deployment, "ExecutorMemory"));
//        addInfoDerivation(new FilialStringDerivation(3, OpenGType.Deployment, "DataInputPath"));
//        addInfoDerivation(new FilialStringDerivation(3, OpenGType.Deployment, "ExecutorSize"));


        addInfoDerivation(new FilialStartTimeDerivation(6));
        addInfoDerivation(new FilialEndTimeDerivation(6));
        addInfoDerivation(new DurationDerivation(7));
        addInfoDerivation(new SummaryDerivation(10));
        addInfoDerivation(new ProcessingTimeDerivation(7));
        addInfoDerivation(new OverallTimeDerivation(8));
//        addInfoDerivation(new ComputationClassDerivation(2));
//        addInfoDerivation(new DatasetDerivation(2));
//        addInfoDerivation(new ContainersLoadedDerivation(2));
//        addInfoDerivation(new ContainerSizeDerivation(2));
        addVisualDerivation(new MainInfoTableVisualization(1,
                new ArrayList<String>() {{
                    add("OverallTime");
                    add("ProcessTime");
                }}));
        addVisualDerivation(new TableVisualization(1, "GeneralInfoTable",
                new ArrayList<String>() {{
//                    add("ApplicationName");
//                    add("DataInputPath");
//                    add("ExecutorMemory");
//                    add("ExecutorSize");
//                    add("ComputationClass");
//                    add("Dataset");
//                    add("ContainersLoaded");
//                    add("ContainerSize");
//                    add("JobStartTime");
//                    add("ArchivingTime");
                }}));

    }


    protected class OverallTimeDerivation extends DerivationRule {

        public OverallTimeDerivation(int level) { super(level); }

        @Override
        public boolean execute() {
            Operation operation = (Operation) entity;
            Info jobDuration = operation.getInfo("Duration");


            List<Source> sources = new ArrayList<>();
            sources.add(new InfoSource(jobDuration.getName(), jobDuration));

            BasicInfo overallTime = new BasicInfo("OverallTime");
            overallTime.setDescription(String.format("The [%s] is the overall time of the job.", overallTime.getName()));
            overallTime.addInfo(jobDuration.getValue(), sources);
            operation.addInfo(overallTime);

            return  true;
        }
    }


    protected class ProcessingTimeDerivation extends DerivationRule {

        public ProcessingTimeDerivation(int level) { super(level); }

        @Override
        public boolean execute() {
            Operation operation = (Operation) entity;
            Operation processGraph = operation.findSuboperation(GiraphType.Job).findSuboperation(GiraphType.ProcessGraph);
            Info processGraphDurationInfo = processGraph.getInfo("Duration");


            List<Source> sources = new ArrayList<>();
            sources.add(new InfoSource(processGraphDurationInfo.getName(), processGraphDurationInfo));

            BasicInfo processingTime = new BasicInfo("ProcessTime");
            processingTime.setDescription(String.format("The [%s] is the time for algorithm execution", processingTime.getName()));
            processingTime.addInfo(processGraphDurationInfo.getValue(), sources);
            operation.addInfo(processingTime);

            return  true;
        }
    }

    protected class SummaryDerivation extends BasicSummaryDerivation {

        public SummaryDerivation(int level) { super(level); }

        @Override
        public boolean execute() {
                Operation operation = (Operation) entity;
                String summary = String.format("The [%s] operation is the top-level operation of each Giraph job. ", operation.getName());
                summary += getBasicSummary(operation);

                SummaryInfo summaryInfo = new SummaryInfo("Summary");
                summaryInfo.setValue("A Summary");
                summaryInfo.addSummary(summary, new ArrayList<Source>());
                operation.addInfo(summaryInfo);
                return  true;
        }
    }



}
