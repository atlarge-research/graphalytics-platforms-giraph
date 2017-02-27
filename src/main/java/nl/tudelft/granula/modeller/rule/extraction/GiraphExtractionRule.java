package nl.tudelft.granula.modeller.rule.extraction;

import nl.tudelft.granula.modeller.rule.extraction.ExtractionRule;
import nl.tudelft.granula.modeller.source.DataStream;
import nl.tudelft.granula.modeller.source.log.Log;
import nl.tudelft.granula.modeller.source.log.LogLocation;
import nl.tudelft.granula.util.UuidGenerator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wing on 21-8-15.
 */
public class GiraphExtractionRule extends ExtractionRule {

    public GiraphExtractionRule(int level) {
        super(level);
    }

    @Override
    public boolean execute() {
        return false;
    }


    public List<Log> extractLogFromInputStream(DataStream dataStream) {

        List<Log> granularlogList = new ArrayList<>();

        try {
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(dataStream.getInputStream()));

            String line = null;
            int lineCount = 0;

            while ((line = br.readLine()) != null) {
                lineCount++;

                if(line.contains("GRANULA") ) {
                    parseGranulaLog(line, lineCount, granularlogList);
                } else if(line.contains("OperationLog")) {
                    parseGrade10Log(line, lineCount, granularlogList);
                }
            }
            br.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return granularlogList;
    }


    private void parseGranulaLog(String line, int lineCount, List<Log> granularlogList) {

        // Skip top-level operation -> it is defined by Grpahalytics benchmark itself, not the platform.
        if (line.contains("Giraph") && line.contains("Job")) {

        } else {

            Log log = extractRecord(line);

            LogLocation trace = new LogLocation();

            String codeLocation;
            String logFilePath;
            codeLocation = "unspecified";
            logFilePath = "unspecified";

            trace.setLocation(logFilePath, lineCount, codeLocation);
            log.setLocation(trace);

            granularlogList.add(log);
        }

    }


    private void parseGrade10Log(String line, int lineCount, List<Log> granularlogList) {

            if(line.contains("ComputeThread")) {
                String infoType = "";
                if(line.contains("event=start")) {
                    infoType="StartTime";
                } else if(line.contains("event=end")) {
                    infoType="EndTime";
                }

                Pattern regex = Pattern.compile(".*time=(\\d*).*worker=(\\d*).*superstep=(\\d*).*thread=compute-(\\d*)");
                Matcher matcher = regex.matcher(line);
                matcher.find();
                String time = matcher.group(1);
                String worker = matcher.group(2);
                String superstep = matcher.group(3);
                String thread = matcher.group(4);

                line = generateText(infoType, time, "WorkerThread", worker + "-" + thread,  "ParallelCompute", superstep, "6796432509645137309");
//                line = String.format("askdjfhsd - GRANULA InfoName:%s InfoValue:%s Timestamp:1487360899995 " +
//                                "RecordUuid:7202475161608429203 OperationUuid:6796432509645137309 ActorType:WorkerThread ActorId:%s MissionType:ParallelCompute MissionId:%s",
//                        infoType, time, worker + "-" + thread, superstep);


                Log log = extractRecord(line);

                LogLocation trace = new LogLocation();

                String codeLocation;
                String logFilePath;
                codeLocation = "unspecified";
                logFilePath = "unspecified";

                trace.setLocation(logFilePath, lineCount, codeLocation);
                log.setLocation(trace);

                granularlogList.add(log);
            }

    }

    private String generateText(String infoName, String infoValue, String actorType, String actorId, String missionType,
                               String missionId, String operationUuid) {
        String text = String.format("GRANULA - InfoName:%s InfoValue:%s ActorType:%s ActorId:%s MissionType:%s MissionId:%s RecordUuid:%s OperationUuid:%s Timestamp:%s\n",
                infoName, infoValue, actorType, actorId, missionType,
                missionId, UuidGenerator.getRandomUUID(), operationUuid, System.currentTimeMillis());
        return text;
    }



    public Log extractRecord(String line) {
        Log log = new Log();

        String granularLog = line.split("GRANULA ")[1];
        String[] logAttrs = granularLog.split("\\s+");

        for (String logAttr : logAttrs) {
            if (logAttr.contains(":")) {
                String[] attrKeyValue = logAttr.split(":");
                if (attrKeyValue.length == 2) {

                    String name = attrKeyValue[0];
                    String value = attrKeyValue[1];
                    String unescapedValue = value.replaceAll("\\[COLON\\]", ":").replaceAll("\\[SPACE\\]", " ");

                    log.addLogInfo(name, unescapedValue);
                } else {
                    log.addLogInfo(attrKeyValue[0], "");
                }
            }
        }
        return log;
    }
}
