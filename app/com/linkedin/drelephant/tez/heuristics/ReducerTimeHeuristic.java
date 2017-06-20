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
import com.linkedin.drelephant.tez.data.TezDAGData;
import com.linkedin.drelephant.tez.data.TezVertexTaskData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.tez.data.TezVertexData;
import com.linkedin.drelephant.util.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.math.Statistics;

import java.util.Map;

import org.apache.log4j.Logger;


public class ReducerTimeHeuristic implements Heuristic<TezDAGApplicationData> {
  private static final Logger logger = Logger.getLogger(ReducerTimeHeuristic.class);

  // Severity parameters.
  private static final String SHORT_RUNTIME_SEVERITY = "short_runtime_severity_in_min";
  private static final String LONG_RUNTIME_SEVERITY = "long_runtime_severity_in_min";
  private static final String NUM_TASKS_SEVERITY = "num_tasks_severity";

  // Default value of parameters
  private double[] shortRuntimeLimits = {10, 4, 2, 1};       // Limits(ms) for tasks with shorter runtime
  private double[] longRuntimeLimits = {15, 30, 60, 120};    // Limits(ms) for tasks with longer runtime
  private double[] numTasksLimits = {50, 101, 500, 1000};    // Number of Reduce tasks.

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confShortRuntimeLimits = Utils.getParam(paramMap.get(SHORT_RUNTIME_SEVERITY), shortRuntimeLimits.length);
    if (confShortRuntimeLimits != null) {
      shortRuntimeLimits = confShortRuntimeLimits;
    }
    logger.info(heuristicName + " will use " + SHORT_RUNTIME_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(shortRuntimeLimits));
    for (int i = 0; i < shortRuntimeLimits.length; i++) {
      shortRuntimeLimits[i] = shortRuntimeLimits[i] * Statistics.MINUTE_IN_MS;
    }

    double[] confLongRuntimeLimitss = Utils.getParam(paramMap.get(LONG_RUNTIME_SEVERITY), longRuntimeLimits.length);
    if (confLongRuntimeLimitss != null) {
      longRuntimeLimits = confLongRuntimeLimitss;
    }
    logger.info(heuristicName + " will use " + LONG_RUNTIME_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(longRuntimeLimits));
    for (int i = 0; i < longRuntimeLimits.length; i++) {
      longRuntimeLimits[i] = longRuntimeLimits[i] * Statistics.MINUTE_IN_MS;
    }

    double[] confNumTasksLimits = Utils.getParam(paramMap.get(NUM_TASKS_SEVERITY), numTasksLimits.length);
    if (confNumTasksLimits != null) {
      numTasksLimits = confNumTasksLimits;
    }
    logger.info(heuristicName + " will use " + NUM_TASKS_SEVERITY + " with the following threshold settings: " + Arrays
        .toString(numTasksLimits));

  }

  public ReducerTimeHeuristic(HeuristicConfigurationData heuristicConfData) {
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

    TezDAGData[] tezDAGsData = data.getTezDAGData();
    TezVertexTaskData[] tasks = null;
 
    List<Long> runTimesMs = new ArrayList<Long>();
    long taskMinMs = Long.MAX_VALUE;
    long taskMaxMs = 0;
    int i=0;
    int taskLength = 0;
/*    List<Long> vRuntimesMs[] = new List[tezVertexes.length];
    String vertexNames[] = new String[tezVertexes.length];
    long vTaskMinMs[] = new long[tezVertexes.length];
    long vTaskMaxMs[] = new long[tezVertexes.length];*/
for(TezDAGData tezDAGData:tezDAGsData){   	
		
    	TezVertexData tezVertexes[] = tezDAGData.getVertexData();
    for (TezVertexData tezVertexData:tezVertexes){
    	tasks = tezVertexData.getReducerData();
    	taskLength+=tasks.length;
    	/*vRuntimesMs[i] = new ArrayList<Long>();
    	vertexNames[i] = tezVertexData.getVertexName();*/
    for (TezVertexTaskData task : tasks) {
    	  

      if (task.isSampled()) {
        long taskTime = task.getTotalRunTimeMs();
        runTimesMs.add(taskTime);
     //   vRuntimesMs[i].add(taskTime);
        taskMinMs = Math.min(taskMinMs, taskTime);
        taskMaxMs = Math.max(taskMaxMs, taskTime);
     /*   vTaskMinMs[i] = Math.min(taskMinMs, taskTime);
        vTaskMaxMs[i] = Math.max(taskMaxMs, taskTime);*/
      
      }
    }
  /*  if(vTaskMinMs[i] == Long.MAX_VALUE) {
    	vTaskMinMs[i] = 0;
      }*/
    i++;
    }
	}

    if(taskMinMs == Long.MAX_VALUE) {
      taskMinMs = 0;
    }

    //Analyze data
    long averageRuntimeMs = Statistics.average(runTimesMs);

    Severity shortTimeSeverity = shortTimeSeverity(averageRuntimeMs, tasks.length);
    Severity longTimeSeverity = longTimeSeverity(averageRuntimeMs, tasks.length);
    Severity severity = Severity.max(shortTimeSeverity, longTimeSeverity);

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), severity, Utils.getHeuristicScore(severity, tasks.length));

/*    long vAverageRuntimeMs[] = new long[tezVertexes.length];

    Severity vShortTimeSeverity[] = new Severity[tezVertexes.length];
    Severity vLongTimeSeverity[] =new Severity[tezVertexes.length];
    Severity vSeverity[] =  new Severity[tezVertexes.length];

    for(int vertexNumber=0;vertexNumber<tezVertexes.length;vertexNumber++){
    	if(vRuntimesMs[vertexNumber].size()>0){
    		
    		vAverageRuntimeMs[vertexNumber] = Statistics.average(vRuntimesMs[vertexNumber]);

    	     vShortTimeSeverity[vertexNumber] = shortTimeSeverity(vAverageRuntimeMs[vertexNumber], vRuntimesMs[vertexNumber].size());
    	     vLongTimeSeverity[vertexNumber] = longTimeSeverity(vAverageRuntimeMs[vertexNumber],  vRuntimesMs[vertexNumber].size());
    	     vSeverity[vertexNumber] = Severity.max(vShortTimeSeverity[vertexNumber], vLongTimeSeverity[vertexNumber]);

    	     if(vSeverity[vertexNumber].getValue()!= 0){
    	    	  result.addResultDetail("Number of tasks in vertex "+vertexNames[vertexNumber], Integer.toString(vRuntimesMs[vertexNumber].size()));
    	    	    result.addResultDetail("Average vertex task runtime "+vertexNames[vertexNumber], Statistics.readableTimespan(vAverageRuntimeMs[vertexNumber]));
    	    	    result.addResultDetail("Max vertex task runtime "+vertexNames[vertexNumber], Statistics.readableTimespan(vTaskMaxMs[vertexNumber]));
    	    	    result.addResultDetail("Min vertex task runtime "+vertexNames[vertexNumber], Statistics.readableTimespan(vTaskMinMs[vertexNumber]));
    	    	  
    	     }
    	     
    	}else{
    		vSeverity[vertexNumber] = Severity.NONE;
    	}
    }*/
    
    result.addResultDetail("Number of output tasks", Integer.toString(tasks.length));
    result.addResultDetail("Average task runtime", Statistics.readableTimespan(averageRuntimeMs));
    result.addResultDetail("Max task runtime", Statistics.readableTimespan(taskMaxMs));
    result.addResultDetail("Min task runtime", Statistics.readableTimespan(taskMinMs));
    return result;
  }

  private Severity shortTimeSeverity(long runtimeMs, long numTasks) {
    Severity timeSeverity = getShortRuntimeSeverity(runtimeMs);
    // Severity is adjusted based on number of tasks
    Severity taskSeverity = getNumTasksSeverity(numTasks);
    return Severity.min(timeSeverity, taskSeverity);
  }

  private Severity longTimeSeverity(long runtimeMs, long numTasks) {
    // Severity is NOT adjusted based on number of tasks
    return getLongRuntimeSeverity(runtimeMs);
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