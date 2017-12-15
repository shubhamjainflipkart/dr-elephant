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

package com.linkedin.drelephant.tez.fetchers;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.tez.data.TezCounterData;
import com.linkedin.drelephant.tez.data.TezDAGApplicationData;
import com.linkedin.drelephant.tez.data.TezDAGData;
import com.linkedin.drelephant.tez.data.TezVertexData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.tez.data.TezVertexTaskData;
import com.linkedin.drelephant.tez.fetchers.ThreadContextTez;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData;
import com.linkedin.drelephant.util.Utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import models.AppResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * This class implements the Fetcher for Tez Applications on Hadoop2
 */
public class TezDataFetcherHadoop2 extends TezDataFetcher {
  private static final Logger logger = Logger.getLogger(TezDataFetcherHadoop2.class);

  private URLFactory _urlFactory;
  private JSONFactory _jsonFactory;
  private String _timelinewebaddress;
  private String _jhistoryWebAddr;
  private String _resourcemanager;
  private String _resourcemanagerWebAddress;
  private String _dagId;
  private static final String SUCCEEDED="SUCCEEDED"; 
  /**
   * Tez Fetcher uses Timeline server data in order to fetch the data. Tez DAG submits data to 
   * the timeline server using events. 
   * @param fetcherConfData
   * @throws IOException
   */
  public TezDataFetcherHadoop2(FetcherConfigurationData fetcherConfData) throws IOException {
    super(fetcherConfData);

    final String timelineaddress = new Configuration().get("yarn.timeline-service.webapp.address");
    final String jhistoryAddr = new Configuration().get("mapreduce.jobhistory.webapp.address");
   
    final String resourcemanager = new Configuration().get("yarn.resourcemanager.webapp.address");
    logger.info("Connecting to the job history server at " + timelineaddress + "...");
    _urlFactory = new URLFactory(timelineaddress);
    logger.info("Connection success.");

    _jsonFactory = new JSONFactory();
    this._timelinewebaddress =  timelineaddress;
    _jhistoryWebAddr = "http://" + jhistoryAddr + "/jobhistory/job/";
    
    _resourcemanager = "http://"+resourcemanager+"/cluster/app/";
    _resourcemanagerWebAddress = "http://"+resourcemanager+"/ws/v1/cluster/apps/";

  }

  /**
   * The fetcher gets the number of dags submitted per application using the diagnostic info from Resource Manager 
   * Then each dag is analyzed and vertex/task information is parsed using the data from timeline server 
   * The analysis is done at the application level and not DAG level. 
   */
  @Override
  public TezDAGApplicationData fetchData(AnalyticJob analyticJob) throws IOException, AuthenticationException {
    String appId = analyticJob.getAppId();
   TezDAGApplicationData jobData = new TezDAGApplicationData();
    String jobId = Utils.getJobIdFromApplicationId(appId);
    jobData.setAppId(appId).setJobId(jobId);
    // Change job tracking url to job history page
    analyticJob.setTrackingUrl(_resourcemanager + appId);
    try {

      // Fetch job config
     Properties jobConf = _jsonFactory.getProperties(_urlFactory.getJobConfigURL(appId));
     jobData.setJobConf(jobConf);
      
      //Fetch number of dags submitted
      int dagCount = getDagSubmittedCount(_urlFactory.getTezDagSubmittedCountURL(appId),jobData);
      

      
      //Right now it only supports DAGs which succeeded. It looks at DAG details to understand wether the application failed or succeded later in the code.
      String state = "SUCCEEDED";


      if (state.equals(SUCCEEDED)) {
    	  
        jobData.setSucceeded(true);
        // Fetch job counter
        
        
        TezVertexData[] tezVertexData=null;
        String dagId = null;
        //TezDAGData[] tezDAGData=new TezDAGData[dagCount];
        List<TezDAGData> tezDAGDataList = new ArrayList<TezDAGData>();
        List<TezVertexTaskData> mapperList = new ArrayList<TezVertexTaskData>();
        List<TezVertexTaskData> reducerList = new ArrayList<TezVertexTaskData>();
        List<TezVertexTaskData> scopeTaskList = new ArrayList<TezVertexTaskData>();
        List<TezVertexData> tezVertexList = new ArrayList<TezVertexData>();
        for(int i=1;i<=dagCount;i++){
        // Fetch task data
        URL taskListURL = _urlFactory.getTaskListURL(jobId);
        TezCounterData jobCounter = _jsonFactory.getJobCounter(_urlFactory.getTezDAGURL(appId, i));
         dagId = _urlFactory.getTezDAGId(appId,i);
        jobData.setCounters(jobCounter);
        TezDAGData tezDAGData = new TezDAGData(jobCounter);
        tezDAGData.setCounter(jobCounter);
        tezDAGData.setDagName(dagId);
        tezDAGData.setTezDAGId(dagId);
        tezDAGDataList.add(tezDAGData);
        _jsonFactory.getTaskDataAll(tezVertexList, mapperList, reducerList,scopeTaskList,_urlFactory.getTezDAGURL(appId, i));
        tezVertexData = tezVertexList.toArray(new TezVertexData[tezVertexList.size()]);
        tezVertexList.clear();
        mapperList.clear();
        reducerList.clear();
        scopeTaskList.clear();
        tezDAGData.setVertexData(tezVertexData);
        }      
        TezDAGData tezDAGDataArray[] = new TezDAGData[tezDAGDataList.size()];
        jobData.setTezDAGData(tezDAGDataList.toArray(tezDAGDataArray));
       
      } 
    } finally {
      ThreadContextTez.updateAuthToken();
    }

    return jobData;
  }

  /**
   * This method looks at how many DAGs were submitted in the application and returns the DAG count
   * @param url
   * @param jobData
   * @return
   * @throws IOException
   * @throws AuthenticationException
   */
  private int getDagSubmittedCount(URL url,TezDAGApplicationData jobData) throws IOException,AuthenticationException{
	    List<AnalyticJob> appList = new ArrayList<AnalyticJob>();

        logger.info("Testing"+url);
	    JsonNode rootNode = ThreadContextTez.readJsonNode(url);
	   
	    String diagnosticsInfo = rootNode.path("app").path("diagnostics").getValueAsText();
	    Long startedTime = (rootNode.path("app").path("startedTime").getLongValue());
	    Long finishedTime = (rootNode.path("app").path("finishedTime").getLongValue());
	    int dagCount = 0;
	    if(diagnosticsInfo!=null){
	    String dagTypes[] = diagnosticsInfo.replace("Session stats:", "").split(",");
	   
	    jobData.setStartTime(startedTime);
	    jobData.setFinishTime(finishedTime);
	   
	    for(String dagType:dagTypes){
	    	
	    	dagCount += Integer.parseInt(dagType.substring((dagType.indexOf("=")+1)).trim());
	    	
	    }
	    } 
	    return dagCount;
	  }
  
  
  

  private URL getTaskCounterURL(String jobId, String taskId) throws MalformedURLException {
    return _urlFactory.getTaskCounterURL(jobId, taskId);
  }

  private URL getTaskAttemptURL(String jobId, String taskId, String attemptId) throws MalformedURLException {
    return _urlFactory.getTaskAttemptURL(jobId, taskId, attemptId);
  }

  private class URLFactory {

    private String _restRoot;
    private String _tezRoot;

    private URLFactory(String hserverAddr) throws IOException {
      _restRoot = "http://" + hserverAddr + "/ws/v1/history/mapreduce/jobs";
      _tezRoot = "http://"+hserverAddr+"/ws/v1/timeline/";
      
      verifyURL(_tezRoot);
    }

    private void verifyURL(String url) throws IOException {
      final URLConnection connection = new URL(url).openConnection();
      // Check service availability
      connection.connect();
      return;
    }

    private URL getJobURL(String jobId) throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId);
    }

    private URL getJobConfigURL(String appId) throws MalformedURLException {
    	 appId = "tez_"+appId;
    	//System.out.println(appId+"appId");
    	appId =_tezRoot+"TEZ_APPLICATION/"+appId;
    	//System.out.println(appId+"appId");
        return new URL(appId);
    }

    private URL getJobCounterURL(String jobId) throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/counters");
    }

    private URL getTaskListURL(String jobId) throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/tasks");
    }

    private URL getTaskCounterURL(String jobId, String taskId) throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/tasks/" + taskId + "/counters");
    }

    private URL getTaskAllAttemptsURL(String jobId, String taskId) throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/tasks/" + taskId + "/attempts");
    }

    private URL getTaskAttemptURL(String jobId, String taskId, String attemptId) throws MalformedURLException {
      return new URL(_restRoot + "/" + jobId + "/tasks/" + taskId + "/attempts/" + attemptId);
    }
    private URL getTezDAGURL(String appId,int id) throws MalformedURLException {
    	String dagId = appId.replace("application","dag");
    //	System.out.println(dagId+"dagId");
    	dagId =_tezRoot+"TEZ_DAG_ID/"+dagId+"_"+id;
    //	System.out.println(dagId+"dagId");
        return new URL(dagId);
      }
    private String getTezDAGId(String appId,int id) throws MalformedURLException {
    	String dagId = appId.replace("application","dag");
    //	System.out.println(dagId+"dagId");
    	dagId =dagId+"_"+id;
    //	System.out.println(dagId+"dagId");
        return (dagId);
      }
    private URL getTezDagSubmittedCountURL(String appId) throws MalformedURLException{
    	return new URL(_resourcemanagerWebAddress+appId);
    }
    private URL getTezDAGURL(String dagId) throws MalformedURLException {
        return new URL(_tezRoot+"TEZ_DAG_ID/"+dagId);
      }
    
     private URL getTezVertexURL(String tezVertexId) throws MalformedURLException {
    
    	 String tezVertexURL = _tezRoot+"TEZ_VERTEX_ID/"+tezVertexId;
        return new URL(tezVertexURL);
      }
     private URL getTezTaskIdURL(String tezTaskId) throws MalformedURLException {
    	 String tezTaskIdURL = _tezRoot+"TEZ_TASK_ID/"+tezTaskId;
         return new URL(tezTaskIdURL);
       }
     
  }

  private class JSONFactory {

    private long getStartTime(URL url) throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextTez.readJsonNode(url);
      return rootNode.path("job").path("startTime").getValueAsLong();
    }

    private long getFinishTime(URL url) throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextTez.readJsonNode(url);
      return rootNode.path("job").path("finishTime").getValueAsLong();
    }

    private long getSubmitTime(URL url) throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextTez.readJsonNode(url);
      return rootNode.path("job").path("submitTime").getValueAsLong();
    }

    private String getState(URL url) throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextTez.readJsonNode(url);
      return rootNode.path("job").path("state").getValueAsText();
    }

    private String getDiagnosticInfo(URL url) throws IOException, AuthenticationException {
      JsonNode rootNode = ThreadContextTez.readJsonNode(url);
      String diag = rootNode.path("job").path("diagnostics").getValueAsText();
      return diag;
    }

    private Properties getProperties(URL url) throws IOException, AuthenticationException {
      Properties jobConf = new Properties();

      JsonNode rootNode = ThreadContextTez.readJsonNode(url);
      JsonNode configs = rootNode.path("otherinfo").get("config");
      Iterator<String> it = configs.getFieldNames();

      //Iterator<Entry<String, JsonNode>> nodeIterator = configs.getFields();
      while (it.hasNext()) {

         String key =  it.next();
         
         String val = configs.get(key).getTextValue();
         jobConf.setProperty(key, val);

      }
   //   System.out.println("finished");

      return jobConf;
    }
    
   
  
 

    private TezCounterData getJobCounter(URL url) throws IOException, AuthenticationException {
      TezCounterData holder = new TezCounterData();

    //  URL tezURL = _urlFactory.getTezDAGURL(_dagId);
    		  //new URL("http://localhost:8188/ws/v1/timeline/TEZ_DAG_ID/dag_1475423301689_0009_1");
      JsonNode rootNodeTez = ThreadContextTez.readJsonNode(url);
      JsonNode groupsTez = rootNodeTez.path("otherinfo").path("counters").path("counterGroups");
      for (JsonNode group : groupsTez) {
          for (JsonNode counter : group.path("counters")) {
            String counterName = counter.get("counterName").getValueAsText();
            Long counterValue = counter.get("counterValue").getLongValue();
            String groupName = group.get("counterGroupName").getValueAsText();
            holder.set(groupName, counterName, counterValue);

          }
        }
      return holder;
    }

   

    private long[] getTaskExecTime(URL url) throws IOException, AuthenticationException {

      JsonNode rootNode = ThreadContextTez.readJsonNode(url);
      JsonNode taskAttempt = rootNode.path("taskAttempt");

      long startTime = taskAttempt.get("startTime").getLongValue();
      long finishTime = taskAttempt.get("finishTime").getLongValue();
      boolean isMapper = taskAttempt.get("type").getValueAsText().equals("MAP");

      long[] time;
      if (isMapper) {
        // No shuffle sore time in Mapper
        time = new long[] { finishTime - startTime, 0, 0 ,startTime, finishTime};
      } else {
        long shuffleTime = taskAttempt.get("elapsedShuffleTime").getLongValue();
        long sortTime = taskAttempt.get("elapsedMergeTime").getLongValue();
        time = new long[] { finishTime - startTime, shuffleTime, sortTime, startTime, finishTime };
      }

      return time;
    }
/**
 * Does all the heavy lifiting for the DAG. Gets information from all the vertexes and tasks for a give DAG. It has a mechanism to determine whether the tasks were used 
 * for reading the data or for processing and writing to HDFS.
 * @param tezVertexList
 * @param mapperList
 * @param reducerList
 * @param scopeTaskList
 * @param dagId
 * @throws IOException
 * @throws AuthenticationException
 */
    private void getTaskDataAll(List<TezVertexData> tezVertexList, List<TezVertexTaskData> mapperList,
        List<TezVertexTaskData> reducerList,List<TezVertexTaskData> scopeTaskList,URL dagId) throws IOException,AuthenticationException {
    	JsonNode rootNode = null;
    	try{
       rootNode = ThreadContextTez.readJsonNode(dagId);
    	}
      catch(FileNotFoundException e){
  		return;
  	}

      JsonNode vertexNode = rootNode.path("otherinfo").path("vertexNameIdMapping");
    //  System.out.println("vertexNode is "+vertexNode);
      Iterator<JsonNode> tasks = vertexNode.iterator();
    //  System.out.println("tasks for vertex"+tasks);
      while (tasks.hasNext()) {
    	JsonNode vertex = tasks.next();
    	
        String vertexId = vertex.getValueAsText();
        System.out.println(" vertex data"+vertexId);

    	TezVertexData tezVertexData = new TezVertexData(vertexId);
    	JsonNode vertexRootNode = ThreadContextTez.readJsonNode(_urlFactory.getTezVertexURL(vertexId));
     //   System.out.println(" vertex root node data"+vertexRootNode);
        
        if(!(vertexRootNode.path("primaryfilters").get("status").getElementValue(0).getTextValue()).contains((SUCCEEDED))){
        	break;
        }
     //   System.out.println(" vertex datasucceded");

        String vertexName = vertexRootNode.path("otherinfo").get("vertexName").getValueAsText();
        tezVertexData.setVertexName(vertexName);
        long startTime = 0l;
        long finishTime = 0l;
        long initialTime = 0l;
        for(JsonNode event:vertexRootNode.path("events") ){
        //	System.out.println("vertexevents"+event);
         	  if("VERTEX_STARTED".equals(event.get("eventtype").getValueAsText())){
         		  
         		  startTime=(event.get("timestamp").getValueAsLong()); 
           //   	System.out.println("vertex start time"+startTime);

         	  }
         	  if("VERTEX_FINISHED".equals(event.get("eventtype").getValueAsText())){
         	     finishTime=(event.get("timestamp").getValueAsLong()); 
           //    	System.out.println("vertex finish time"+finishTime);

         	  }
         	  if("VERTEX_INITIALIZED".equals(event.get("eventtype").getValueAsText())){
         	      initialTime = (event.get("timestamp").getValueAsLong()); 
         	  }
         	  
           }
        long time [] =  { finishTime - startTime, 0, 0, startTime, finishTime };
        tezVertexData.setTime(time);
        JsonNode groupsTez = vertexRootNode.path("otherinfo").path("counters").path("counterGroups");
        TezCounterData holder = new TezCounterData();
        
        for (JsonNode group : groupsTez) {
            for (JsonNode counter : group.path("counters")) {
              String counterName = counter.get("counterName").getValueAsText();
              Long counterValue = counter.get("counterValue").getLongValue();
              String groupName = group.get("counterGroupName").getValueAsText();
              holder.set(groupName, counterName, counterValue);
            //  System.out.println("counter"+counterName+counterValue+groupName);

            }
          }
        tezVertexData.setCounter(holder);
        JsonNode tasksNode = vertexRootNode.path("relatedentities").get("TEZ_TASK_ID");
     //   System.out.println("taskNode"+tasksNode);
        Iterator<JsonNode> iterator = (tasksNode == null)?null:tasksNode.getElements();
     //   System.out.println("iterator"+iterator);
        while(iterator != null && iterator.hasNext()){
        	JsonNode taskNode = iterator.next(); 
        	String taskId=taskNode.getValueAsText();
       //     System.out.println("taskId is "+taskId);

        	JsonNode tezTaskRootNode = ThreadContextTez.readJsonNode(_urlFactory.getTezTaskIdURL(taskId));
       //    System.out.println("task root node  "+tezTaskRootNode);

        	if(!(tezTaskRootNode.path("primaryfilters").get("status").getElementValue(0).getTextValue()).contains(SUCCEEDED)){
            	break;
            }
        	
       //    System.out.println("task succeeded");

        	TezVertexTaskData mapReduceTaskData = new TezVertexTaskData(taskId,
        			tezTaskRootNode.path("relatedentities").get("TEZ_TASK_ATTEMPT_ID").getValueAsText());
        //    System.out.println("mapreduce data"+mapReduceTaskData);
        	if(vertexRootNode.path("otherinfo").get("vertexName").getValueAsText().contains("Map")){
        	mapperList.add(mapReduceTaskData);
        	
        	}else if (vertexRootNode.path("otherinfo").get("vertexName").getValueAsText().contains("scope")){
        		scopeTaskList.add(mapReduceTaskData);
        	}else{
        		reducerList.add(mapReduceTaskData);
        	}
        	 for(JsonNode event:tezTaskRootNode.path("events") ){
            	  if("TASK_STARTED".equals(event.get("eventtype").getValueAsText())){
            		  startTime=(event.get("timestamp").getValueAsLong()); 
            	  }
            	  if("TASK_FINISHED".equals(event.get("eventtype").getValueAsText())){
            	     finishTime=(event.get("timestamp").getValueAsLong()); 
            	  }
            	             	  
              }
           long taskTime [] =  { finishTime - startTime, 0, 0, startTime, finishTime };
           mapReduceTaskData.setTime(taskTime);
           JsonNode groupsTezTask = tezTaskRootNode.path("otherinfo").path("counters").path("counterGroups");
           TezCounterData holderTask = new TezCounterData();
           for (JsonNode group : groupsTezTask) {
               for (JsonNode counter : group.path("counters")) {
                 String counterName = counter.get("counterName").getValueAsText();
                 Long counterValue = counter.get("counterValue").getLongValue();
                 String groupName = group.get("counterGroupName").getValueAsText();
                 holderTask.set(groupName, counterName, counterValue);
            //     System.out.println("counter task"+counterName+counterValue);


               }
             }
           mapReduceTaskData.setCounter(holderTask); 
           
        }
        TezVertexTaskData mapperData [] = mapperList.toArray(new TezVertexTaskData[mapperList.size()]);
        TezVertexTaskData reducerData [] = reducerList.toArray(new TezVertexTaskData[reducerList.size()]);
        TezVertexTaskData scopeTaskData [] = scopeTaskList.toArray(new TezVertexTaskData[scopeTaskList.size()]);

        tezVertexData.setMapperData(mapperData);
        tezVertexData.setReducerData(reducerData);
        tezVertexData.setScopeTaskData(scopeTaskData);
        mapperList.clear();
        reducerList.clear();
        scopeTaskList.clear();
    	tezVertexList.add(tezVertexData);

      }

    }

   
  }

public String getDagId() {
	return _dagId;
}

public void setDagId(String _dagId) {
	this._dagId = _dagId;
}
}

final class ThreadContextTez {
  private static final Logger logger = Logger.getLogger(ThreadContextTez.class);
  private static final AtomicInteger THREAD_ID = new AtomicInteger(1);

  private static final ThreadLocal<Integer> _LOCAL_THREAD_ID = new ThreadLocal<Integer>() {
    @Override
    public Integer initialValue() {
      return THREAD_ID.getAndIncrement();
    }
  };

  private static final ThreadLocal<Long> _LOCAL_LAST_UPDATED = new ThreadLocal<Long>();
  private static final ThreadLocal<Long> _LOCAL_UPDATE_INTERVAL = new ThreadLocal<Long>();

  private static final ThreadLocal<Pattern> _LOCAL_DIAGNOSTIC_PATTERN = new ThreadLocal<Pattern>() {
    @Override
    public Pattern initialValue() {
      // Example: "Task task_1443068695259_9143_m_000475 failed 1 times"
      return Pattern.compile(
          "Task[\\s\\u00A0]+(.*)[\\s\\u00A0]+failed[\\s\\u00A0]+([0-9])[\\s\\u00A0]+times[\\s\\u00A0]+");
    }
  };

  private static final ThreadLocal<AuthenticatedURL.Token> _LOCAL_AUTH_TOKEN =
      new ThreadLocal<AuthenticatedURL.Token>() {
        @Override
        public AuthenticatedURL.Token initialValue() {
          _LOCAL_LAST_UPDATED.set(System.currentTimeMillis());
          // Random an interval for each executor to avoid update token at the same time
          _LOCAL_UPDATE_INTERVAL.set(Statistics.MINUTE_IN_MS * 30 + new Random().nextLong()
              % (3 * Statistics.MINUTE_IN_MS));
          logger.info("Executor " + _LOCAL_THREAD_ID.get() + " update interval " + _LOCAL_UPDATE_INTERVAL.get() * 1.0
              / Statistics.MINUTE_IN_MS);
          return new AuthenticatedURL.Token();
        }
      };

  private static final ThreadLocal<AuthenticatedURL> _LOCAL_AUTH_URL = new ThreadLocal<AuthenticatedURL>() {
    @Override
    public AuthenticatedURL initialValue() {
      return new AuthenticatedURL();
    }
  };

  private static final ThreadLocal<ObjectMapper> _LOCAL_MAPPER = new ThreadLocal<ObjectMapper>() {
    @Override
    public ObjectMapper initialValue() {
      return new ObjectMapper();
    }
  };

  private ThreadContextTez() {
    // Empty on purpose
  }

  public static Matcher getDiagnosticMatcher(String diagnosticInfo) {
    return _LOCAL_DIAGNOSTIC_PATTERN.get().matcher(diagnosticInfo);
  }

  public static JsonNode readJsonNode(URL url) throws IOException, AuthenticationException {

    HttpURLConnection conn = _LOCAL_AUTH_URL.get().openConnection(url, _LOCAL_AUTH_TOKEN.get());
    logger.info("Testing"+conn);
    return _LOCAL_MAPPER.get().readTree(conn.getInputStream());
  }

  public static void updateAuthToken() {
    long curTime = System.currentTimeMillis();
    if (curTime - _LOCAL_LAST_UPDATED.get() > _LOCAL_UPDATE_INTERVAL.get()) {
      logger.info("Executor " + _LOCAL_THREAD_ID.get() + " updates its AuthenticatedToken.");
      _LOCAL_AUTH_TOKEN.set(new AuthenticatedURL.Token());
      _LOCAL_AUTH_URL.set(new AuthenticatedURL());
      _LOCAL_LAST_UPDATED.set(curTime);
    }
  }
}
