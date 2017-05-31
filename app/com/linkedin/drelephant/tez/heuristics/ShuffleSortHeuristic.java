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

import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.tez.data.TezDAGData;
import com.linkedin.drelephant.tez.data.TezVertexTaskData;
import com.linkedin.drelephant.tez.data.TezDAGApplicationData;
import com.linkedin.drelephant.tez.data.TezVertexData;
import com.linkedin.drelephant.math.Statistics;

import java.util.Map;

import org.apache.log4j.Logger;


/**
 * Analyses the efficiency of Shuffle and Sort
 */
public class ShuffleSortHeuristic implements Heuristic<TezDAGApplicationData> {
  private static final Logger logger = Logger.getLogger(ShuffleSortHeuristic.class);

  // Severity parameters.
  private static final String RUNTIME_RATIO_SEVERITY = "runtime_ratio_severity";
  private static final String RUNTIME_SEVERITY = "runtime_severity_in_min";

  // Default value of parameters
  private double[] runtimeRatioLimits = {1, 2, 4, 8};       // Avg Shuffle or Sort Time * 2 / Avg Exec Time
  private double[] runtimeLimits = {1, 5, 10, 30};          // Shuffle/Sort Runtime in milli sec

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confRatioLimitsd = Utils.getParam(paramMap.get(RUNTIME_RATIO_SEVERITY), runtimeRatioLimits.length);
    if (confRatioLimitsd != null) {
      runtimeRatioLimits = confRatioLimitsd;
    }
    logger.info(heuristicName + " will use " + RUNTIME_RATIO_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(runtimeRatioLimits));

    double[] confRuntimeLimits = Utils.getParam(paramMap.get(RUNTIME_SEVERITY), runtimeLimits.length);
    if (confRuntimeLimits != null) {
      runtimeLimits = confRuntimeLimits;
    }
    logger.info(heuristicName + " will use " + RUNTIME_SEVERITY + " with the following threshold settings: " + Arrays
        .toString(runtimeLimits));
    for (int i = 0; i < runtimeLimits.length; i++) {
      runtimeLimits[i] = runtimeLimits[i] * Statistics.MINUTE_IN_MS;
    }
  }

  public ShuffleSortHeuristic(HeuristicConfigurationData heuristicConfData) {
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
    
    

    TezVertexTaskData[] tasks = null;

    List<Long> execTimeMs = new ArrayList<Long>();
    List<Long> shuffleTimeMs = new ArrayList<Long>();
    List<Long> sortTimeMs = new ArrayList<Long>();
    
   /* List<Long> vExecTimeMs[] = new List[tezVertexes.length];
    List<Long> vShuffleTimeMs[] = new List[tezVertexes.length];
    List<Long> vSortTimeMs[] = new List[tezVertexes.length];
    String vertexNames[] = new String[tezVertexes.length];*/
    TezDAGData[] tezDAGsData = data.getTezDAGData();
  int i = 0;
  int taskLength = 0;
  for(TezDAGData tezDAGData:tezDAGsData){ 
	  TezVertexData tezVertexes[] = tezDAGData.getVertexData();
    for (TezVertexData tezVertexData:tezVertexes){
/*    	vExecTimeMs[i] = new ArrayList<Long>();
    	vShuffleTimeMs[i] = new ArrayList<Long>();
    	vSortTimeMs[i] = new ArrayList<Long>();
    	*/
    	//vtaskPmin[i] = Long.MAX_VALUE;
    	 tasks = tezVertexData.getTasksData();
    	 taskLength+=tasks.length;
    	// vertexNames[i] = tezVertexData.getVertexName();
    	 for (TezVertexTaskData task : tasks) {
    		 if (task.isSampled()) {
    		        execTimeMs.add(task.getCodeExecutionTimeMs());
    		        shuffleTimeMs.add(task.getShuffleTimeMs());
    		        sortTimeMs.add(task.getSortTimeMs());
    		        
    		        /*vExecTimeMs[i].add(task.getCodeExecutionTimeMs());
    		        vShuffleTimeMs[i].add(task.getShuffleTimeMs());
    		        vSortTimeMs[i] .add(task.getSortTimeMs());*/
    		       
    		      } 
    	 }
    	 
    i++;
    }
    
  }

    //Analyze data
    long avgExecTimeMs = Statistics.average(execTimeMs);
    long avgShuffleTimeMs = Statistics.average(shuffleTimeMs);
    long avgSortTimeMs = Statistics.average(sortTimeMs);

    Severity shuffleSeverity = getShuffleSortSeverity(avgShuffleTimeMs, avgExecTimeMs);
    Severity sortSeverity = getShuffleSortSeverity(avgSortTimeMs, avgExecTimeMs);
    Severity severity = Severity.max(shuffleSeverity, sortSeverity);

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), severity, Utils.getHeuristicScore(severity, tasks.length));

   /* 
    //Analyze data
    long vAvgExecTimeMs[] =  new long[tezVertexes.length];
    long vAvgShuffleTimeMs[] =  new long[tezVertexes.length];
    long vAvgSortTimeMs[] =  new long[tezVertexes.length];

    Severity vShuffleSeverity[] =  new Severity[tezVertexes.length];
    Severity vSortSeverity[] =  new Severity[tezVertexes.length];
    Severity vSeverity[] =  new Severity[tezVertexes.length];
    
    for(int vertexNumber=0;vertexNumber<tezVertexes.length;vertexNumber++){
    	vAvgExecTimeMs[vertexNumber] = Statistics.average(vExecTimeMs[vertexNumber]);
    	vAvgShuffleTimeMs[vertexNumber] = Statistics.average(vShuffleTimeMs[vertexNumber]);
    	vAvgSortTimeMs[vertexNumber] = Statistics.average(vSortTimeMs[vertexNumber]);
    	if (vExecTimeMs[vertexNumber].size() == 0) {
    	      vSeverity[vertexNumber] = Severity.NONE;
    	    } else {
    	    	
    	   vShuffleSeverity[vertexNumber] = getShuffleSortSeverity(vAvgShuffleTimeMs[vertexNumber], vAvgExecTimeMs[vertexNumber]);
    	     vSortSeverity[vertexNumber] = getShuffleSortSeverity(vAvgSortTimeMs[vertexNumber], vAvgExecTimeMs[vertexNumber]);
    	     vSeverity[vertexNumber] = Severity.max(vShuffleSeverity[vertexNumber], vSortSeverity[vertexNumber]);
    	     if(vSeverity[vertexNumber].getValue()!= 0){
    	    	 result.addResultDetail("Number of vertex tasks in "+vertexNames[vertexNumber], Integer.toString(vExecTimeMs[vertexNumber].size()));
    	    	    result.addResultDetail("Average vertex code runtime "+vertexNames[vertexNumber], Statistics.readableTimespan(vAvgExecTimeMs[vertexNumber]));
    	    	    String vShuffleFactor = Statistics.describeFactor(vAvgShuffleTimeMs[vertexNumber], vAvgExecTimeMs[vertexNumber], "x");
    	    	    result.addResultDetail("Average vertex shuffle time "+vertexNames[vertexNumber], Statistics.readableTimespan(vAvgShuffleTimeMs[vertexNumber]) + " " + vShuffleFactor);
    	    	    String vSortFactor = Statistics.describeFactor(vAvgSortTimeMs[vertexNumber], vAvgExecTimeMs[vertexNumber], "x");
    	    	    result.addResultDetail("Average vertex sort time "+vertexNames[vertexNumber], Statistics.readableTimespan(vAvgSortTimeMs[vertexNumber]) + " " + vSortFactor);
 
    	     }
    	    }
    }*/
    
    result.addResultDetail("Number of tasks", Integer.toString(taskLength));
    result.addResultDetail("Average code runtime", Statistics.readableTimespan(avgExecTimeMs));
    String shuffleFactor = Statistics.describeFactor(avgShuffleTimeMs, avgExecTimeMs, "x");
    result.addResultDetail("Average shuffle time", Statistics.readableTimespan(avgShuffleTimeMs) + " " + shuffleFactor);
    String sortFactor = Statistics.describeFactor(avgSortTimeMs, avgExecTimeMs, "x");
    result.addResultDetail("Average sort time", Statistics.readableTimespan(avgSortTimeMs) + " " + sortFactor);

    return result;
  }

  private Severity getShuffleSortSeverity(long runtimeMs, long codetimeMs) {
    Severity runtimeSeverity = Severity.getSeverityAscending(
        runtimeMs, runtimeLimits[0], runtimeLimits[1], runtimeLimits[2], runtimeLimits[3]);

    if (codetimeMs <= 0) {
      return runtimeSeverity;
    }
    long value = runtimeMs * 2 / codetimeMs;

    Severity runtimeRatioSeverity = Severity.getSeverityAscending(
        value, runtimeRatioLimits[0], runtimeRatioLimits[1], runtimeRatioLimits[2], runtimeRatioLimits[3]);

    return Severity.min(runtimeSeverity, runtimeRatioSeverity);
  }
}
