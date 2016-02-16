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
package nl.tudelft.graphalytics.giraph.reporting.logging;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;

/**
 * Created by wlngai on 9-9-15.
 */
public class GiraphLogger extends GraphalyticLogger {

    protected static Level platformLogLevel = Level.INFO;

    public static void startPlatformLogging(Path fileName) {
        Logger.getRootLogger().removeAllAppenders();
        FileAppender fa = new FileAppender();
        fa.setName("FileLogger");
        fa.setFile(fileName.toString());
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
