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
package nl.tudelft.graphalytics.giraph.reporting.logging;

import nl.tudelft.graphalytics.reporting.logging.GraphalyticLogger;
import org.apache.hadoop.yarn.client.cli.LogsCLI;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wlngai on 9-9-15.
 */
public class GiraphLogger extends GraphalyticLogger {

    protected static Level platformLogLevel = Level.INFO;

    public static void startPlatformLogging(String fileName) {
        Logger.getRootLogger().removeAllAppenders();
        FileAppender fa = new FileAppender();
        fa.setName("FileLogger");
        fa.setFile(fileName);
        fa.setLayout(new PatternLayout("%d [%t] %-5p[%c{1} (%M(%L))] %m%n"));
        fa.setThreshold(platformLogLevel);
        fa.setAppend(true);
        fa.activateOptions();
        Logger.getRootLogger().addAppender(fa);
        waitInterval(1);
    }

    public static void stopPlatformLogging() {
        Logger.getRootLogger().removeAllAppenders();
    }

}
