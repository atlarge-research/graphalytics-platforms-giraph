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
