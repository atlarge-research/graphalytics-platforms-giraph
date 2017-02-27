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

package nl.tudelft.granula.modeller.rule.linking;

import nl.tudelft.granula.modeller.Type;
import nl.tudelft.granula.modeller.platform.operation.Operation;
import nl.tudelft.granula.modeller.entity.BasicType;

import java.util.ArrayList;
import java.util.List;

public class AdvancedIdentifierParentLinking extends LinkingRule {

    String parentActorType;
    String parentActorId;
    String parentMissionType;
    String parentMissionId;

    public AdvancedIdentifierParentLinking(String parentActorType, String parentActorId,
                                           String parentMissionType, String parentMissionId) {
        super();
        this.parentActorType = parentActorType;
        this.parentActorId = parentActorId;
        this.parentMissionType = parentMissionType;
        this.parentMissionId = parentMissionId;
    }

    @Override
    public boolean execute() {

        Operation operation = (Operation) entity;

        List<Operation> matchedParents = new ArrayList<>();

        for (Operation candidateOperation : operation.getPlatform().getOperations()) {

            boolean actorMatched = candidateOperation.getActor().getType().equals(parentActorType);
            boolean missionMatched = candidateOperation.getMission().getType().equals(parentMissionType);

            if(!actorMatched || !missionMatched) {
                continue;
            }

            boolean actorIdMatched = false;
            if(parentActorId.equals(BasicType.Unique)) {
                actorIdMatched = true;
            } else if(parentActorId.equals(BasicType.Equal)) {
                actorIdMatched = candidateOperation.getActor().getId().equals(operation.getActor().getId());
            } else if(parentActorId.equals(BasicType.Any)) {
                actorIdMatched = true;
            } else if (parentActorId.equals(Type.Ladder)) {
                // matching by "starts with". like 10-22 is the parent of 10-22-54
                actorIdMatched = operation.getActor().getId().startsWith(candidateOperation.getActor().getId() + "-");
            } else if(parentActorId.equals(candidateOperation.getActor().getId())) {
                actorIdMatched = true;
            } else {
                if(operation.containsLog(parentActorId)) {
                    String matchedParentActorId = String.valueOf(operation.getLog(parentActorId).getAttr("InfoValue"));
                    actorIdMatched = candidateOperation.getActor().getId().equals(matchedParentActorId);
                }
            }

            boolean missionIdMatched = false;
            if(parentMissionId.equals(BasicType.Unique)) {
                missionIdMatched = true;
            } else if(parentMissionId.equals(BasicType.Equal)) {
                missionIdMatched = candidateOperation.getMission().getId().equals(operation.getMission().getId());
            } else if(parentMissionId.equals(BasicType.Any)) {
                missionIdMatched = true;
            } else if (parentActorId.equals(Type.Ladder)) {
                // matching by "starts with". like 10-22 is the parent of 10-22-54
                actorIdMatched = operation.getMission().getId().startsWith(candidateOperation.getMission().getId() + "-");
            } else if(parentMissionId.equals(candidateOperation.getMission().getId())) {
                missionIdMatched = true;
            } else {
                if(operation.containsLog(parentMissionId)) {
                    String matchedParentMissionId = String.valueOf(operation.getLog(parentMissionId).getAttr("InfoValue"));
                    missionIdMatched = candidateOperation.getMission().getId().equals(matchedParentMissionId);
                }
            }

            if (actorMatched && actorIdMatched && missionMatched && missionIdMatched) {
                matchedParents.add(candidateOperation);
            }
        }

        if(matchedParents.size() != 1) {
            throw new IllegalStateException(String.format("Parent not found %s %s %s %s",
                    parentActorType, parentActorId, parentMissionType, parentMissionId));
        }

        Operation parent = matchedParents.get(0);
        operation.setParent(parent);
        parent.addChild(operation);
        return  true;
    }

    @Override
    public String toString() {
        return "AdvancedIdentifierParentLinking{" +
                "operation=" + ((Operation) entity).getName() +
                "parentActorType='" + parentActorType + '\'' +
                ", parentActorId='" + parentActorId + '\'' +
                ", parentMissionType='" + parentMissionType + '\'' +
                ", parentMissionId='" + parentMissionId + '\'' +
                '}';
    }
}
