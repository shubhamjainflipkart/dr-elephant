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

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.tez.data.TezCounterData;

import java.util.Properties;


/**
 * This class contains the Tez Application Information
 */
public class TezDAGApplicationData implements HadoopApplicationData {
  private static final ApplicationType APPLICATION_TYPE = new ApplicationType("TEZ");

  private boolean _succeeded = true;
  private String _diagnosticInfo = "";
  private String _appId = "";
  private String _jobId = "";
  private String _username = "";
  private String _url = "";
  private String _jobName = "";
  private long _submitTime = 0;
  private long _startTime = 0;
  private long _finishTime = 0;

  private TezCounterData _counterHolder;
  private TezDAGData[] _tezDAGData;
  private Properties _jobConf;
  private boolean _isRetry = false;

  public TezDAGApplicationData setSucceeded(boolean succeeded) {
    this._succeeded = succeeded;
    return this;
  }

  public TezDAGApplicationData setDiagnosticInfo(String diagnosticInfo) {
    this._diagnosticInfo = diagnosticInfo;
    return this;
  }

  public TezDAGApplicationData setRetry(boolean isRetry) {
    this._isRetry = isRetry;
    return this;
  }

  public TezDAGApplicationData setAppId(String appId) {
    this._appId = appId;
    return this;
  }

  public TezDAGApplicationData setJobId(String jobId) {
    this._jobId = jobId;
    return this;
  }

  public TezDAGApplicationData setJobName(String jobName) {
    this._jobName = jobName;
    return this;
  }

  public TezDAGApplicationData setUsername(String username) {
    this._username = username;
    return this;
  }

  public TezDAGApplicationData setSubmitTime(long submitTime) {
    this._submitTime = submitTime;
    return this;
  }

  public TezDAGApplicationData setStartTime(long startTime) {
    this._startTime = startTime;
    return this;
  }

  public TezDAGApplicationData setFinishTime(long finishTime) {
    this._finishTime = finishTime;
    return this;
  }

  public TezDAGApplicationData setUrl(String url) {
    this._url = url;
    return this;
  }

  public TezDAGApplicationData setCounters(TezCounterData jobCounter) {
    this._counterHolder = jobCounter;
    return this;
  }

  

  public TezDAGApplicationData setJobConf(Properties jobConf) {
    this._jobConf = jobConf;
    return this;
  }

  public TezCounterData getCounters() {
    return _counterHolder;
  }
  


  @Override
  public String getAppId() {
    return _appId;
  }

  @Override
  public Properties getConf() {
    return _jobConf;
  }

  @Override
  public ApplicationType getApplicationType() {
    return APPLICATION_TYPE;
  }

  @Override
  public boolean isEmpty() {
    return _succeeded && getTezDAGData().length == 0;
  }

  public String getUsername() {
    return _username;
  }

  public long getSubmitTime() {
    return _submitTime;
  }

  public long getStartTime() {
    return _startTime;
  }

  public long getFinishTime() {
    return _finishTime;
  }

  public String getUrl() {
    return _url;
  }

  public String getJobName() {
    return _jobName;
  }

  public boolean isRetryJob() {
    return _isRetry;
  }

  public String getJobId() {
    return _jobId;
  }

  public boolean getSucceeded() {
    return _succeeded;
  }

  public String getDiagnosticInfo() {
    return _diagnosticInfo;
  }

  public TezDAGData[] getTezDAGData() {
	return _tezDAGData;
}

public void setTezDAGData(TezDAGData[] tezDAGData) {
	this._tezDAGData = tezDAGData;
}

@Override
  public String toString() {
    return "id: " + getJobId() + ", name:" + getJobName();
  }
}
