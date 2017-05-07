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


/**
 * This class manages the MapReduce Tasks
 */
public class TezVertexTaskData {
  private TezCounterData _counterHolder;
  private String _taskId;
  // The successful attempt id
  private String _attemptId;
  private long _totalTimeMs = 0;
  private long _shuffleTimeMs = 0;
  private long _sortTimeMs = 0;
  private long _startTimeMs = 0;
  private long _finishTimeMs = 0;
  private boolean _sampled = false;

  public TezVertexTaskData(TezCounterData counterHolder, long[] time) {
    this._counterHolder = counterHolder;
    this._totalTimeMs = time[0];
    this._shuffleTimeMs = time[1];
    this._sortTimeMs = time[2];
    this._startTimeMs = time[3];
    this._finishTimeMs = time[4];
    this._sampled = true;
  }

  public TezVertexTaskData(TezCounterData counterHolder) {
    this._counterHolder = counterHolder;
  }

  public TezVertexTaskData(String taskId, String taskAttemptId) {
    this._taskId = taskId;
    this._attemptId = taskAttemptId;
  }

  public TezVertexTaskData(String taskId) {
	  this._taskId = taskId;
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

  public String getTaskId() {
    return _taskId;
  }

  public String getAttemptId() {
    return _attemptId;
  }
}
