/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.tez.heuristics;

import com.linkedin.drelephant.tez.data.TezDAGApplicationData;
import com.linkedin.drelephant.tez.data.TezCounterData;
import com.linkedin.drelephant.tez.data.TezVertexData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.tez.data.TezVertexTaskData;
import com.linkedin.drelephant.math.Statistics;

import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class VertexScopeTaskTimeHeuristic implements Heuristic<TezDAGApplicationData> {
  private static final Logger logger = Logger.getLogger(VertexScopeTaskTimeHeuristic.class);

  // Severity parameters.
  private static final String SHORT_RUNTIME_SEVERITY = "short_runtime_severity_in_min";
  private static final String LONG_RUNTIME_SEVERITY = "long_runtime_severity_in_min";
  private static final String NUM_TASKS_SEVERITY = "num_tasks_severity";

  // Default value of parameters
  private double[] shortRuntimeLimits = {10, 4, 2, 1};     // Limits(ms) for tasks with shorter runtime
  private double[] longRuntimeLimits = {15, 30, 60, 120};  // Limits(ms) for tasks with longer runtime
  private double[] numTasksLimits = {50, 101, 500, 1000};  // Number of Map tasks.

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confShortThreshold = Utils.getParam(paramMap.get(SHORT_RUNTIME_SEVERITY), shortRuntimeLimits.length);
    if (confShortThreshold != null) {
      shortRuntimeLimits = confShortThreshold;
    }
    logger.info(heuristicName + " will use " + SHORT_RUNTIME_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(shortRuntimeLimits));
    for (int i = 0; i < shortRuntimeLimits.length; i++) {
      shortRuntimeLimits[i] = shortRuntimeLimits[i] * Statistics.MINUTE_IN_MS;
    }

    double[] confLongThreshold = Utils.getParam(paramMap.get(LONG_RUNTIME_SEVERITY), longRuntimeLimits.length);
    if (confLongThreshold != null) {
      longRuntimeLimits = confLongThreshold;
    }
    logger.info(heuristicName + " will use " + LONG_RUNTIME_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(longRuntimeLimits));
    for (int i = 0; i < longRuntimeLimits.length; i++) {
      longRuntimeLimits[i] = longRuntimeLimits[i] * Statistics.MINUTE_IN_MS;
    }

    double[] confNumTasksThreshold = Utils.getParam(paramMap.get(NUM_TASKS_SEVERITY), numTasksLimits.length);
    if (confNumTasksThreshold != null) {
      numTasksLimits = confNumTasksThreshold;
    }
    logger.info(heuristicName + " will use " + NUM_TASKS_SEVERITY + " with the following threshold settings: " + Arrays
        .toString(numTasksLimits));
  }

  public VertexScopeTaskTimeHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
    loadParameters();
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(TezDAGApplicationData data) {

    if(!data.getSucceeded()) {
      return null;
    }

    TezVertexData tezVertexes[] = data.getTezVertexData();
    TezVertexTaskData[] tasks = null;
 
    List<Long> inputBytes = new ArrayList<Long>();
    List<Long> runtimesMs = new ArrayList<Long>();
    
    List<Long> vInputBytes[] = new List[tezVertexes.length];
    List<Long> vRuntimesMs[] = new List[tezVertexes.length];
    String vertexNames[] = new String[tezVertexes.length];

    long taskMinMs = Long.MAX_VALUE;
    long taskMaxMs = 0;
    
    long vTaskMinMs[] = new long[tezVertexes.length];
    long vTaskMaxMs[] = new long[tezVertexes.length];
    
    int i=0;
    int taskLength = 0;
    for (TezVertexData tezVertexData:tezVertexes){
    	tasks = tezVertexData.getScopeTaskData();
    
    	vTaskMinMs[i] = Long.MAX_VALUE;
    	vTaskMinMs[i] = 0;
    	vRuntimesMs[i] = new ArrayList<Long>();
    	vInputBytes[i] = new ArrayList<Long>();
    	 vertexNames[i] = tezVertexData.getVertexName();
    for (TezVertexTaskData task : tasks) {
  	  taskLength+=tasks.length;
  
    
      if (task.isSampled()) {
        inputBytes.add(task.getCounters().get(TezCounterData.CounterName.HDFS_BYTES_READ));
        long taskTime = task.getTotalRunTimeMs();
        runtimesMs.add(taskTime);
        taskMinMs = Math.min(taskMinMs, taskTime);
        taskMaxMs = Math.max(taskMaxMs, taskTime);
        
        vRuntimesMs[i].add(taskTime);
        vTaskMinMs[i] = Math.min(vTaskMinMs[i], taskTime);
        vTaskMaxMs[i] = Math.max(vTaskMaxMs[i], taskTime);
      }
    }
    if(vTaskMinMs[i] == Long.MAX_VALUE) {
        vTaskMinMs[i] = 0;
      }
	i++;
    }

    if(taskMinMs == Long.MAX_VALUE) {
      taskMinMs = 0;
    }

    long averageSize = Statistics.average(inputBytes);
    long averageTimeMs = Statistics.average(runtimesMs);

    Severity shortTaskSeverity = shortTaskSeverity(tasks.length, averageTimeMs);
    Severity longTaskSeverity = longTaskSeverity(tasks.length, averageTimeMs);
    Severity severity = Severity.max(shortTaskSeverity, longTaskSeverity);

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), severity, Utils.getHeuristicScore(severity, tasks.length));
    
    
    long vAverageSize[] = new long[tezVertexes.length];
    long vAverageTimeMs[] = new long[tezVertexes.length];

    Severity vShortTaskSeverity[] = new Severity[tezVertexes.length];
    Severity vLongTaskSeverity[] = new Severity[tezVertexes.length];
    Severity vSeverity[] = new Severity[tezVertexes.length];
    
    for(int vertexNumber=0;vertexNumber<tezVertexes.length;vertexNumber++){
    	if(vRuntimesMs[vertexNumber].size()>0){
    		vAverageSize[vertexNumber] = Statistics.average(vInputBytes[vertexNumber]);
    		vAverageTimeMs[vertexNumber] = Statistics.average(vRuntimesMs[vertexNumber]);
    		 vShortTaskSeverity[vertexNumber] = shortTaskSeverity(vRuntimesMs[vertexNumber].size(), vAverageTimeMs[vertexNumber]);
    		 vLongTaskSeverity[vertexNumber] = longTaskSeverity(vRuntimesMs[vertexNumber].size(), vAverageTimeMs[vertexNumber]);
     		vSeverity[vertexNumber] = Severity.NONE;
    		 vSeverity[vertexNumber] = Severity.max(vShortTaskSeverity[vertexNumber], vLongTaskSeverity[vertexNumber]);
    		   if(vSeverity[vertexNumber].getValue()!= 0){
    		  result.addResultDetail("Number of tasks in vertex "+vertexNames[vertexNumber], Integer.toString(vRuntimesMs[vertexNumber].size()));
  	    	    result.addResultDetail("Average vertex task input size "+vertexNames[vertexNumber], FileUtils.byteCountToDisplaySize(vAverageSize[vertexNumber]));
    		    result.addResultDetail("Average task runtime "+vertexNames[vertexNumber], Statistics.readableTimespan(vAverageTimeMs[vertexNumber]));
    		    result.addResultDetail("Max task runtime "+vertexNames[vertexNumber], Statistics.readableTimespan(vTaskMaxMs[vertexNumber]));
    		    result.addResultDetail("Min task runtime "+vertexNames[vertexNumber], Statistics.readableTimespan(vTaskMinMs[vertexNumber]));

    		   }
    	}else{
    		vSeverity[vertexNumber] = Severity.NONE;
    	}
    }
    		
    result.addResultDetail("Number of map vertices", Integer.toString(i));
    result.addResultDetail("Number of  tasks", Integer.toString(taskLength));
    result.addResultDetail("Average task input size", FileUtils.byteCountToDisplaySize(averageSize));
    result.addResultDetail("Average task runtime", Statistics.readableTimespan(averageTimeMs));
    result.addResultDetail("Max task runtime", Statistics.readableTimespan(taskMaxMs));
    result.addResultDetail("Min task runtime", Statistics.readableTimespan(taskMinMs));

    return result;
  }

  private Severity shortTaskSeverity(long numTasks, long averageTimeMs) {
    // We want to identify jobs with short task runtime
    Severity severity = getShortRuntimeSeverity(averageTimeMs);
    // Severity is reduced if number of tasks is small.
    Severity numTaskSeverity = getNumTasksSeverity(numTasks);
    return Severity.min(severity, numTaskSeverity);
  }

  private Severity longTaskSeverity(long numTasks, long averageTimeMs) {
    // We want to identify jobs with long task runtime. Severity is NOT reduced if num of tasks is large
    return getLongRuntimeSeverity(averageTimeMs);
  }

  private Severity getShortRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityDescending(
            runtimeMs, shortRuntimeLimits[0], shortRuntimeLimits[1], shortRuntimeLimits[2], shortRuntimeLimits[3]);
  }

  private Severity getLongRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityAscending(
        runtimeMs, longRuntimeLimits[0], longRuntimeLimits[1], longRuntimeLimits[2], longRuntimeLimits[3]);
  }

  private Severity getNumTasksSeverity(long numTasks) {
    return Severity.getSeverityAscending(
        numTasks, numTasksLimits[0], numTasksLimits[1], numTasksLimits[2], numTasksLimits[3]);
  }
}
