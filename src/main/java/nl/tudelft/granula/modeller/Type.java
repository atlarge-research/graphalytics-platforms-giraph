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

package nl.tudelft.granula.modeller;

import nl.tudelft.granula.modeller.entity.BasicType;

public class Type extends BasicType {

    // actor
    public static String Giraph = "Giraph";
    public static String Worker = "Worker";
    public static String WorkerThread = "WorkerThread";

    // mission
    public static String Job = "Job";
    public static String Setup = "Setup";
    public static String PostCompute = "PostCompute";
    public static String PreCompute = "PreCompute";
    public static String Postpare = "Postpare";
    public static String Prepare = "Prepare";
    public static String Execute = "Execute";
    public static String PreApplication = "PreApplication";
    public static String PostApplication = "PostApplication";
    public static String Compute = "Compute";
    public static String LocalSuperstep = "LocalSuperstep";
    public static String Superstep = "Superstep";
    public static String ParallelCompute = "ParallelCompute";
    public static String VertexCompute = "VertexCompute";

    // info

    // others
    public static String Ladder = "Ladder";
}