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

package com.linkedin.drelephant.tez.data;

import com.linkedin.drelephant.tez.data.TezVertexData;


/**
 * This class manages the Tez Vertex data
 * 
 */
public class TezDAGData {
  private TezCounterData _counterHolder;
  private String _tezDAGId;
  private String dagName; 
  // The successful attempt id
  private String _attemptId;
  private long _totalTimeMs = 0;
  private long _shuffleTimeMs = 0;
  private long _sortTimeMs = 0;
  private long _startTimeMs = 0;
  private long _finishTimeMs = 0;
  private boolean _sampled = false;
  private TezVertexData[] _vertexData;
  
 
  public TezDAGData(TezCounterData counterHolder, long[] time) {
    this._counterHolder = counterHolder;
    this._totalTimeMs = time[0];
    this._shuffleTimeMs = time[1];
    this._sortTimeMs = time[2];
    this._startTimeMs = time[3];
    this._finishTimeMs = time[4];
    this._sampled = true;
  }

  public TezDAGData(TezCounterData counterHolder) {
    this._counterHolder = counterHolder;
  }

  

public void setCounter(TezCounterData counterHolder) {
    this._counterHolder = counterHolder;
    this._sampled = true;
  }

  public void setTime(long[] time) {
    this._totalTimeMs = time[0];
    this._shuffleTimeMs = time[1];
    this._sortTimeMs = time[2];
    this._startTimeMs = time[3];
    this._finishTimeMs = time[4];
    this._sampled = true;
  }

  public TezCounterData getCounters() {
    return _counterHolder;
  }

  public long getTotalRunTimeMs() {
    return _totalTimeMs;
  }

  public long getCodeExecutionTimeMs() {
    return _totalTimeMs - _shuffleTimeMs - _sortTimeMs;
  }

  public long getShuffleTimeMs() {
    return _shuffleTimeMs;
  }

  public long getSortTimeMs() {
    return _sortTimeMs;
  }

  public long getStartTimeMs() {
    return _startTimeMs;
  }

  public long getFinishTimeMs() {
    return _finishTimeMs;
  }

  public boolean isSampled() {
    return _sampled;
  }

  
  public String getAttemptId() {
    return _attemptId;
  }






public TezVertexData[] getVertexData() {
	return _vertexData;
}

public void setVertexData(TezVertexData[] vertexData) {
	this._vertexData = vertexData;
}

public String getTezDAGId() {
	return _tezDAGId;
}

public void setTezDAGId(String _tezDAGId) {
	this._tezDAGId = _tezDAGId;
}

public String getDagName() {
	return dagName;
}

public void setDagName(String dagName) {
	this.dagName = dagName;
}

}
