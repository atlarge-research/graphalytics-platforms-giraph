package nl.tudelft.granula.modeller;

import nl.tudelft.granula.archiver.GranulaExecutor;
import nl.tudelft.granula.modeller.entity.Execution;
import nl.tudelft.granula.modeller.job.JobModel;
import nl.tudelft.granula.modeller.platform.Giraph;
import nl.tudelft.granula.util.FileUtil;
import nl.tudelft.granula.util.json.JsonUtil;

import java.nio.file.Paths;

/**
 * Created by wing on 21-8-15.
 */
public class ModelTester {
    public static void main(String[] args) {
        String inputPath = "/home/wlngai/Workstation/Exec/Granula/debug/archiver/giraph/log";
        String outputPath = "/home/wlngai/Workstation/Exec/Granula/debug/archiver/giraph/arc";

        Execution execution = (Execution) JsonUtil.fromJson(FileUtil.readFile(
                Paths.get(inputPath + "/execution/execution-log.js")), Execution.class);
        execution.setLogPath(inputPath);
        // Set end time in "log directory"/execution/execution-log.js, or the end time is set as the current time.
        execution.setEndTime(execution.getStartTime() + 90000);
        execution.setArcPath(outputPath);
        JobModel jobModel = new JobModel(new Giraph());

        GranulaExecutor granulaExecutor = new GranulaExecutor();
        granulaExecutor.setEnvEnabled(false);
        granulaExecutor.setExecution(execution);
        granulaExecutor.buildJobArchive(jobModel);
    }
}
