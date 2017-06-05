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
import com.linkedin.drelephant.tez.data.TezDAGData;
import com.linkedin.drelephant.tez.data.TezVertexData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.linkedin.drelephant.analysis.HDFSContext;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.tez.data.TezVertexTaskData;
import com.linkedin.drelephant.math.Statistics;

import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class TezVertexMapSpeedHeuristic implements Heuristic<TezDAGApplicationData> {
  private static final Logger logger = Logger.getLogger(TezVertexMapSpeedHeuristic.class);

  // Severity parameters.
  private static final String DISK_SPEED_SEVERITY = "disk_speed_severity";
  private static final String RUNTIME_SEVERITY = "runtime_severity_in_min";

  // Default value of parameters
  private double[] diskSpeedLimits = {1d/2, 1d/4, 1d/8, 1d/32};  // Fraction of HDFS block size
  private double[] runtimeLimits = {5, 10, 15, 30};              // The Map task runtime in milli sec

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] confDiskSpeedThreshold = Utils.getParam(paramMap.get(DISK_SPEED_SEVERITY), diskSpeedLimits.length);
    if (confDiskSpeedThreshold != null) {
      diskSpeedLimits = confDiskSpeedThreshold;
    }
    logger.info(heuristicName + " will use " + DISK_SPEED_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(diskSpeedLimits));
    for (int i = 0; i < diskSpeedLimits.length; i++) {
      diskSpeedLimits[i] = diskSpeedLimits[i] * HDFSContext.DISK_READ_SPEED;
    }

    double[] confRuntimeThreshold = Utils.getParam(paramMap.get(RUNTIME_SEVERITY), runtimeLimits.length);
    if (confRuntimeThreshold != null) {
      runtimeLimits = confRuntimeThreshold;
    }
    logger.info(heuristicName + " will use " + RUNTIME_SEVERITY + " with the following threshold settings: " + Arrays
        .toString(runtimeLimits));
    for (int i = 0; i < runtimeLimits.length; i++) {
      runtimeLimits[i] = runtimeLimits[i] * Statistics.MINUTE_IN_MS;
    }
  }

  public TezVertexMapSpeedHeuristic(HeuristicConfigurationData heuristicConfData) {
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
    TezDAGData[] tezDAGsData = data.getTezDAGData();
   //  TezVertexTaskData[] tasks = null;
 

    List<Long> inputByteSizes = new ArrayList<Long>();
    List<Long> speeds = new ArrayList<Long>();
    List<Long> runtimesMs = new ArrayList<Long>();
    int i=0;
    int taskLength = 0;
  /*  List<Long> vInputByteSizes[] = new List[tezVertexes.length];
    List<Long> vSpeeds[] = new List[tezVertexes.length];
    List<Long> vRuntimesMs[] = new List[tezVertexes.length];
    String vertexNames[] = new String[tezVertexes.length];
  */ 
    for(TezDAGData tezDAGData:tezDAGsData){   	
		
    	TezVertexData tezVertexes[] = tezDAGData.getVertexData();
    for (TezVertexData tezVertexData:tezVertexes){
    	tasks = tezVertexData.getMapperData();
    	
    	taskLength+=tasks.length;
    	/*vInputByteSizes[i] = new ArrayList<Long>();
    	vSpeeds[i] = new ArrayList<Long>();
    	vRuntimesMs[i] = new ArrayList<Long>();
    	vertexNames[i] = tezVertexData.getVertexName();*/
    for (TezVertexTaskData task : tasks) {

      if (task.isSampled()) {
    	  
        long inputBytes = task.getCounters().get(TezCounterData.CounterName.HDFS_BYTES_READ);
        long runtimeMs = task.getTotalRunTimeMs();
        inputByteSizes.add(inputBytes);
        runtimesMs.add(runtimeMs);
        //Speed is bytes per second
        speeds.add((1000 * inputBytes) / (runtimeMs));
      /*  vInputByteSizes[i].add(inputBytes);
        vSpeeds[i].add((1000 * inputBytes) / (runtimeMs));
        vRuntimesMs[i].add(runtimeMs);*/
        
        
      }
    }
    i++;
    }
  }

    long medianSpeed;
    long medianSize;
    long medianRuntimeMs;

    if (tasks.length != 0) {
      medianSpeed = Statistics.median(speeds);
      medianSize = Statistics.median(inputByteSizes);
      medianRuntimeMs = Statistics.median(runtimesMs);
    } else {
      medianSpeed = 0;
      medianSize = 0;
      medianRuntimeMs = 0;
    }

    Severity severity = getDiskSpeedSeverity(medianSpeed);

    //This reduces severity if task runtime is insignificant
    severity = Severity.min(severity, getRuntimeSeverity(medianRuntimeMs));

    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
        _heuristicConfData.getHeuristicName(), severity, Utils.getHeuristicScore(severity, i));
/*
    long vMedianSpeed[] = new long[tezVertexes.length];
    long vMedianSize[] = new long[tezVertexes.length];
    long vMedianRuntimeMs[] = new long[tezVertexes.length];
    Severity vSeverity[] = new Severity[tezVertexes.length];
    
    for(int vertexNumber=0;vertexNumber<tezVertexes.length;vertexNumber++){
    	if(vRuntimesMs[vertexNumber].size()>0){
    	vMedianSpeed[vertexNumber] = Statistics.median(vSpeeds[vertexNumber]);
    	vMedianSize[vertexNumber] = Statistics.median(vInputByteSizes[vertexNumber]);
    	vMedianRuntimeMs[vertexNumber] = Statistics.median(vRuntimesMs[vertexNumber]);
    	}else 
    	{
    		vMedianSpeed[vertexNumber] = 0;
        	vMedianSize[vertexNumber] = 0;
        	vMedianRuntimeMs[vertexNumber] = 0;
    	}
    	if (vRuntimesMs[vertexNumber].size() == 0) {
  	      vSeverity[vertexNumber] = Severity.NONE;
  	    } else {
  	    	
  	    	vSeverity[vertexNumber] = getDiskSpeedSeverity(vMedianSpeed[vertexNumber]);
  	    	vSeverity[vertexNumber] = Severity.min(vSeverity[vertexNumber], getRuntimeSeverity(vMedianRuntimeMs[vertexNumber] ));
  	    	 if(vSeverity[vertexNumber].getValue()!=0){
  	    		 
  	    		result.addResultDetail("Number of tasks in vertex "+vertexNames[vertexNumber], Integer.toString(vRuntimesMs[vertexNumber].size()));
  	    	    result.addResultDetail("Median task input size in vertex "+vertexNames[vertexNumber], FileUtils.byteCountToDisplaySize(vMedianSize[vertexNumber]));
  	    	    result.addResultDetail("Median task runtime in vertex "+vertexNames[vertexNumber], Statistics.readableTimespan(vMedianRuntimeMs[vertexNumber]));
  	    	    result.addResultDetail("Median task speed in vertex"+vertexNames[vertexNumber], FileUtils.byteCountToDisplaySize(vMedianSpeed[vertexNumber]) + "/s");

  	    	 }
  	    }
    	
    }*/
    
    result.addResultDetail("Number of vertices", Integer.toString(i));
    result.addResultDetail("Number of  tasks", Integer.toString(taskLength));

    result.addResultDetail("Median task input size", FileUtils.byteCountToDisplaySize(medianSize));
    result.addResultDetail("Median task runtime", Statistics.readableTimespan(medianRuntimeMs));
    result.addResultDetail("Median task speed", FileUtils.byteCountToDisplaySize(medianSpeed) + "/s");

    return result;
  }

  private Severity getDiskSpeedSeverity(long speed) {
    return Severity.getSeverityDescending(
        speed, diskSpeedLimits[0], diskSpeedLimits[1], diskSpeedLimits[2], diskSpeedLimits[3]);
  }

  private Severity getRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityAscending(
        runtimeMs,  runtimeLimits[0], runtimeLimits[1], runtimeLimits[2], runtimeLimits[3]);
  }
}
