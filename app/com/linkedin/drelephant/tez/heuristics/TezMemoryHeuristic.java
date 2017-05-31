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
import com.linkedin.drelephant.tez.data.TezCounterData;
import com.linkedin.drelephant.tez.data.TezDAGApplicationData;
import com.linkedin.drelephant.tez.data.TezDAGData;
import com.linkedin.drelephant.tez.data.TezVertexTaskData;
import com.linkedin.drelephant.tez.data.TezVertexData;
import com.linkedin.drelephant.math.Statistics;

import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


/**
 * This heuristic deals with the efficiency of container size
 */
public  class TezMemoryHeuristic implements Heuristic<TezDAGApplicationData> {
  private static final Logger logger = Logger.getLogger(TezMemoryHeuristic.class);
  private static final long CONTAINER_MEMORY_DEFAULT_BYTES = 2048L * FileUtils.ONE_MB;
  public static final String CONTAINER_MEMORY_CONF = "tez.am.resource.memory.mb";
  // Severity Parameters
  private static final String MEM_RATIO_SEVERITY = "memory_ratio_severity";
  private static final String CONTAINER_MEM_SEVERITY = "container_memory_severity";
  private static final String CONTAINER_MEM_DEFAULT_MB = "container_memory_default_mb";

  // Default value of parameters
  private double[] memRatioLimits = {0.6d, 0.5d, 0.4d, 0.3d}; // Avg Physical Mem of Tasks / Container Mem
  private double[] memoryLimits = {1.1d, 1.5d, 2.0d, 2.5d};   // Container Memory Severity Limits

  private String _containerMemConf = "tez.am.resource.memory.mb";
  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confMemRatioLimits = Utils.getParam(paramMap.get(MEM_RATIO_SEVERITY), memRatioLimits.length);
    if (confMemRatioLimits != null) {
      memRatioLimits = confMemRatioLimits;
    }
    logger.info(heuristicName + " will use " + MEM_RATIO_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(memRatioLimits));

    long containerMemDefaultBytes = CONTAINER_MEMORY_DEFAULT_BYTES;
    if (paramMap.containsKey(CONTAINER_MEM_DEFAULT_MB)) {
      containerMemDefaultBytes = Long.valueOf(paramMap.get(CONTAINER_MEM_DEFAULT_MB)) * FileUtils.ONE_MB;
    }
    logger.info(heuristicName + " will use " + CONTAINER_MEM_DEFAULT_MB + " with the following threshold setting: "
            + containerMemDefaultBytes);

    double[] confMemoryLimits = Utils.getParam(paramMap.get(CONTAINER_MEM_SEVERITY), memoryLimits.length);
    if (confMemoryLimits != null) {
      memoryLimits = confMemoryLimits;
    }
    logger.info(heuristicName + " will use " + CONTAINER_MEM_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(memoryLimits));
    for (int i = 0; i < memoryLimits.length; i++) {
      memoryLimits[i] = memoryLimits[i] * containerMemDefaultBytes;
    }
  }

  public TezMemoryHeuristic(HeuristicConfigurationData heuristicConfData) {
	  super();
	    this._containerMemConf = "tez.am.resource.memory.mb";
	    this._heuristicConfData = heuristicConfData;

	    loadParameters();
	  }
	/*

  protected  MapReduceTaskData[] getTasks(TezDAGApplicationData data){
	  
  }*/

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  @Override
  public HeuristicResult apply(TezDAGApplicationData data) {
	  
    if(!data.getSucceeded()) {
      return null;
    }

    String containerSizeStr = data.getConf().getProperty(_containerMemConf);
    if (containerSizeStr == null) {
      return null;
    }

    long containerMem;
    try {
      containerMem = Long.parseLong(containerSizeStr);
    } catch (NumberFormatException e) {
      // Some job has a string var like "${VAR}" for this config.
      if(containerSizeStr.startsWith("$")) {
        String realContainerConf = containerSizeStr.substring(containerSizeStr.indexOf("{")+1,
            containerSizeStr.indexOf("}"));
        containerMem = Long.parseLong(data.getConf().getProperty(realContainerConf));
      } else {
        throw e;
      }
    }
    containerMem *= FileUtils.ONE_MB;
    

    int i=0;
    TezDAGData[] tezDAGsData = data.getTezDAGData();
    
    TezVertexTaskData[] tasks = null;
  /*  long pVertexMem[] = new long[tezVertexes.length];
    long vVertexMem[] = new long[tezVertexes.length];
  */ 
    List<Long> taskPMems = new ArrayList<Long>();
    List<Long> taskVMems = new ArrayList<Long>();
    List<Long> runtimesMs = new ArrayList<Long>();
 /*   List<Long> vTaskPMems[] = new List[tezVertexes.length];
    List<Long> vTaskVMems[] = new List[tezVertexes.length];
    List<Long> vRuntimesMs[] = new List[tezVertexes.length];*/
    long taskPMin = Long.MAX_VALUE;
    long taskPMax = 0;
  /*  long vtaskPmin[] = new long[tezVertexes.length];
    long vtaskPmax[] = new long[tezVertexes.length];
    String vertexNames[] = new String[tezVertexes.length];*/
    int taskLength = 0;
   for(TezDAGData tezDAGData:tezDAGsData){   	
		
    	TezVertexData tezVertexes[] = tezDAGData.getVertexData();
    for (TezVertexData tezVertexData:tezVertexes){
    	/*vTaskPMems[i] = new ArrayList<Long>();
    	vTaskVMems[i] = new ArrayList<Long>();
    	vRuntimesMs[i] = new ArrayList<Long>();*/
    	
    	//vtaskPmin[i] = Long.MAX_VALUE;
    	 tasks = tezVertexData.getTasksData();
    	 taskLength+=tasks.length;
    	// vertexNames[i] = tezVertexData.getVertexName();
    	 for (TezVertexTaskData task : tasks) {
    	      if (task.isSampled()) {
    	        runtimesMs.add(task.getTotalRunTimeMs());
    	       // vRuntimesMs[i].add(task.getTotalRunTimeMs());
    	        long taskPMem = task.getCounters().get(TezCounterData.CounterName.PHYSICAL_MEMORY_BYTES);
    	        long taskVMem = task.getCounters().get(TezCounterData.CounterName.VIRTUAL_MEMORY_BYTES);
    	        taskPMems.add(taskPMem);
    	      //  vTaskPMems[i].add(taskPMem);
    	        taskPMin = Math.min(taskPMin, taskPMem);
    	        taskPMax = Math.max(taskPMax, taskPMem);
    	       // vtaskPmin[i] = Math.min(taskPMin, taskPMem);
    	      //  vtaskPmax[i] = Math.max(taskPMax, taskPMem);
    	        taskVMems.add(taskVMem);
    	       // vTaskVMems[i].add(taskVMem);
    	      }
    	    }

    	    if(taskPMin == Long.MAX_VALUE) {
    	      taskPMin = 0;
    	   //   vtaskPmin[i]=0;
    	    }
    	    
    	 i++;
    }
  }
    
    long taskPMemAvg = Statistics.average(taskPMems);
    long taskVMemAvg = Statistics.average(taskVMems);
    long averageTimeMs = Statistics.average(runtimesMs);
    
    Severity severity;
    if (tasks.length == 0) {
      severity = Severity.NONE;
    } else {
      severity = getTaskMemoryUtilSeverity(taskPMemAvg, containerMem);
    }

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), severity, Utils.getHeuristicScore(severity, tasks.length));
    
   /* long vTaskPMemAvg[] = new long[tezVertexes.length];
    long vTaskVMemAvg[] = new long[tezVertexes.length];
    long vAverageTimeMs[] = new long[tezVertexes.length];
    Severity vSeverity[] = new Severity[tezVertexes.length];
    */
    
    
    result.addResultDetail("Total Number of vertexes", Integer.toString(i));
    result.addResultDetail("Total Number of tasks", Integer.toString(taskLength));
    result.addResultDetail("Avg task runtime", Statistics.readableTimespan(averageTimeMs));
    result.addResultDetail("Avg Physical Memory (MB)", Long.toString(taskPMemAvg / FileUtils.ONE_MB));
    result.addResultDetail("Max Physical Memory (MB)", Long.toString(taskPMax / FileUtils.ONE_MB));
    result.addResultDetail("Min Physical Memory (MB)", Long.toString(taskPMin / FileUtils.ONE_MB));
    result.addResultDetail("Avg Virtual Memory (MB)", Long.toString(taskVMemAvg / FileUtils.ONE_MB));
    result.addResultDetail("Requested Container Memory", FileUtils.byteCountToDisplaySize(containerMem));
/*
    for(int vertexNumber=0;vertexNumber<tezVertexes.length;vertexNumber++){ 
    	vTaskPMemAvg[vertexNumber]  = Statistics.average(vTaskPMems[vertexNumber]);
    	vTaskVMemAvg[vertexNumber]  = Statistics.average(vTaskVMems[vertexNumber]);
    	vAverageTimeMs[vertexNumber]  = Statistics.average(vRuntimesMs[vertexNumber]);
    	if (vTaskVMems[vertexNumber].size() == 0) {
    	      vSeverity[vertexNumber] = Severity.NONE;
    	    } else {
    	    	 vSeverity[vertexNumber] = getTaskMemoryUtilSeverity(taskPMemAvg, containerMem);
    	    	 if(vSeverity[vertexNumber].getValue()!= 0){
    	    		 result.addResultDetail("Number of tasks in vertex "+vertexNames[vertexNumber], Integer.toString(vTaskVMems[vertexNumber].size()));
    	    	    	result.addResultDetail("Avg Vertex Task Runtime "+vertexNames[vertexNumber], Statistics.readableTimespan(vAverageTimeMs[vertexNumber]));
    	    	        result.addResultDetail("Avg Vertex Physical Memory (MB) "+vertexNames[vertexNumber], Long.toString(vTaskPMemAvg[vertexNumber] / FileUtils.ONE_MB));
    	    	        result.addResultDetail("Max Vertex Physical Memory (MB) "+vertexNames[vertexNumber], Long.toString(vtaskPmax[vertexNumber] / FileUtils.ONE_MB));
    	    	        result.addResultDetail("Min Vertex Physical Memory (MB) "+vertexNames[vertexNumber], Long.toString(vtaskPmin[vertexNumber] / FileUtils.ONE_MB));
    	    	        result.addResultDetail("Avg Vertex Virtual Memory (MB) "+vertexNames[vertexNumber], Long.toString(vTaskVMemAvg[vertexNumber] / FileUtils.ONE_MB));	 
    	    	 }
    	    }
    	
    }*/
    return result;
  }

  private Severity getTaskMemoryUtilSeverity(long taskMemAvg, long taskMemMax) {
    double ratio = ((double)taskMemAvg) / taskMemMax;
    Severity sevRatio = getMemoryRatioSeverity(ratio);
    // Severity is reduced if the requested container memory is close to default
    Severity sevMax = getContainerMemorySeverity(taskMemMax);

    return Severity.min(sevRatio, sevMax);
  }


  private Severity getContainerMemorySeverity(long taskMemMax) {
    return Severity.getSeverityAscending(
        taskMemMax, memoryLimits[0], memoryLimits[1], memoryLimits[2], memoryLimits[3]);
  }

  private Severity getMemoryRatioSeverity(double ratio) {
    return Severity.getSeverityDescending(
        ratio, memRatioLimits[0], memRatioLimits[1], memRatioLimits[2], memRatioLimits[3]);
  }
}
