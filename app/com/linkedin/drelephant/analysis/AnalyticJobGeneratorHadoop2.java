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

package com.linkedin.drelephant.analysis;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.SqlUpdate;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.ElephantRunner;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.security.HadoopSecurity;
import controllers.MetricsController;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import models.AppResult;
import models.CheckPoint;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * This class provides a list of analysis promises to be generated under Hadoop YARN environment
 */
public class AnalyticJobGeneratorHadoop2 implements AnalyticJobGenerator {
  private static final Logger logger = Logger.getLogger(AnalyticJobGeneratorHadoop2.class);
  private static final String RESOURCE_MANAGER_ADDRESS = "yarn.resourcemanager.webapp.address";
  private static final String IS_RM_HA_ENABLED = "yarn.resourcemanager.ha.enabled";
  private static final String RESOURCE_MANAGER_IDS = "yarn.resourcemanager.ha.rm-ids";
  private static final String RM_NODE_STATE_URL = "http://%s/ws/v1/cluster/info";
  private static final String FETCH_INITIAL_WINDOW_MS = "drelephant.analysis.fetch.initial.windowMillis";

  private static Configuration configuration;

  // We provide one minute job fetch delay due to the job sending lag from AM/NM to JobHistoryServer HDFS
  private static final long FETCH_DELAY = 60000;

  // Generate a token update interval with a random deviation so that it does not update the token exactly at the same
  // time with other token updaters (e.g. ElephantFetchers).
  private static final long TOKEN_UPDATE_INTERVAL =
      Statistics.MINUTE_IN_MS * 30 + new Random().nextLong() % (3 * Statistics.MINUTE_IN_MS);

  private String _resourceManagerAddress;
  private long _lastRun;
  private long _lastTime = 0;
  private long _fetchStartTime = 0;
  private long _currentTime = 0;
  private long _tokenUpdatedTime = 0;
  private HadoopSecurity _hadoopSecurity;
  private AuthenticatedURL.Token _token;
  private AuthenticatedURL _authenticatedURL;
  private final ObjectMapper _objectMapper = new ObjectMapper();

  public void fetchAndExecuteJobs(long checkPoint) {

    updateResourceManagerAddresses();

    _lastRun = System.currentTimeMillis();
    logger.info("Fetching analytic job list...");
    _hadoopSecurity = ElephantRunner.getInstance().getHadoopSecurity();
    while (true) {
      try {
        _hadoopSecurity.checkLogin();
        break;
      } catch (IOException e) {
        logger.info("Error with hadoop kerberos login", e);
        waitInterval(ElephantRunner.getInstance().getRetryInterval());
      }
    }

    _lastTime = checkPoint;
    List<AnalyticJob> todos;
    while (true) {
      try {
        todos = fetchAnalyticJobs();
        logger.info("jobs count: " + todos.size());
        break;
      } catch (Exception e) {
        logger.error("Error fetching job list. Try again later...", e);
        waitInterval(ElephantRunner.getInstance().getRetryInterval());
      }
    }

    for (AnalyticJob analyticJob : todos) {
      ElephantRunner.getInstance().getDistributedExecutorService().execute(analyticJob);
    }
  }

  public void updateResourceManagerAddresses() {
    if (Boolean.valueOf(configuration.get(IS_RM_HA_ENABLED))) {
      String resourceManagers = configuration.get(RESOURCE_MANAGER_IDS);
      if (resourceManagers != null) {
        logger.info("The list of RM IDs are " + resourceManagers);
        List<String> ids = Arrays.asList(resourceManagers.split(","));
        _currentTime = System.currentTimeMillis();
        updateAuthToken();
        for (String id : ids) {
          try {
            String resourceManager = configuration.get(RESOURCE_MANAGER_ADDRESS + "." + id);
            String resourceManagerURL = String.format(RM_NODE_STATE_URL, resourceManager);
            logger.info("Checking RM URL: " + resourceManagerURL);
            JsonNode rootNode = readJsonNode(new URL(resourceManagerURL));
            String status = rootNode.path("clusterInfo").path("haState").getValueAsText();
            if (status.equals("ACTIVE")) {
              logger.info(resourceManager + " is ACTIVE");
              _resourceManagerAddress = resourceManager;
              break;
            } else {
              logger.info(resourceManager + " is STANDBY");
            }
          } catch (AuthenticationException e) {
            logger.info("Error fetching resource manager " + id + " state " + e.getMessage());
          } catch (IOException e) {
            logger.info("Error fetching Json for resource manager "+ id + " status " + e.getMessage());
          }
        }
      }
    } else {
      _resourceManagerAddress = configuration.get(RESOURCE_MANAGER_ADDRESS);
    }
    if (_resourceManagerAddress == null) {
      throw new RuntimeException(
              "Cannot get YARN resource manager address from Hadoop Configuration property: [" + RESOURCE_MANAGER_ADDRESS
                      + "].");
    }
  }

  @Override
  public void configure(Configuration configuration)
      throws IOException {
    this.configuration = configuration;
    String initialFetchWindowString = configuration.get(FETCH_INITIAL_WINDOW_MS);
    if (initialFetchWindowString != null) {
      long initialFetchWindow = Long.getLong(initialFetchWindowString);
      _lastTime = System.currentTimeMillis() - FETCH_DELAY - initialFetchWindow;
      _fetchStartTime = _lastTime;
    }
    updateResourceManagerAddresses();
  }

  /**
   *  Fetch all the succeeded and failed applications/analytic jobs from the resource manager.
   *
   * @return
   * @throws IOException
   * @throws AuthenticationException
   */
  private List<AnalyticJob> fetchAnalyticJobs()
      throws IOException, AuthenticationException {
    List<AnalyticJob> appList = new ArrayList<AnalyticJob>();

    // There is a lag of job data from AM/NM to JobHistoryServer HDFS, we shouldn't use the current time, since there
    // might be new jobs arriving after we fetch jobs. We provide one minute delay to address this lag.
    _currentTime = System.currentTimeMillis() - FETCH_DELAY;
    updateAuthToken();

    logger.info("Fetching recent finished application runs between last time: " + (_lastTime + 1)
        + ", and current time: " + _currentTime);

    // Fetch all succeeded apps
    URL succeededAppsURL = new URL(new URL("http://" + _resourceManagerAddress), String.format(
            "/ws/v1/cluster/apps?finalStatus=SUCCEEDED&finishedTimeBegin=%s&finishedTimeEnd=%s",
            String.valueOf(_lastTime + 1), String.valueOf(_currentTime)));
    logger.info("The succeeded apps URL is " + succeededAppsURL);
    List<AnalyticJob> succeededApps = readApps(succeededAppsURL);
    appList.addAll(succeededApps);

    // Fetch all failed apps
    // state: Application Master State
    // finalStatus: Status of the Application as reported by the Application Master
    URL failedAppsURL = new URL(new URL("http://" + _resourceManagerAddress), String.format(
        "/ws/v1/cluster/apps?finalStatus=FAILED&state=FINISHED&finishedTimeBegin=%s&finishedTimeEnd=%s",
        String.valueOf(_lastTime + 1), String.valueOf(_currentTime)));
    List<AnalyticJob> failedApps = readApps(failedAppsURL);
    logger.info("The failed apps URL is " + failedAppsURL);
    appList.addAll(failedApps);

    updateCheckPoint();

    return appList;
  }

  @Override
  public void jobAnalysis(AnalyticJob analyticJob) {
    try {
      String analysisName = String.format("%s %s", analyticJob.getAppType().getName(), analyticJob.getAppId());
      long analysisStartTimeMillis = System.currentTimeMillis();
      logger.info(String.format("Analyzing %s", analysisName));
      AppResult result = analyticJob.getAnalysis();
      result.save();
      long processingTime = System.currentTimeMillis() - analysisStartTimeMillis;
      logger.info(String.format("Analysis of %s took %sms", analysisName, processingTime));
      MetricsController.setJobProcessingTime(processingTime);
      MetricsController.markProcessedJobs();

    } catch (InterruptedException e) {
      logger.info("Thread interrupted");
      logger.info(e.getMessage());
      logger.info(ExceptionUtils.getStackTrace(e));

      Thread.currentThread().interrupt();
    } catch (Exception e) {
      logger.error(e.getMessage());
      logger.error(ExceptionUtils.getStackTrace(e));

      if (analyticJob != null && analyticJob.retry()) {
        logger.error("Add analytic job id [" + analyticJob.getAppId() + "] into the retry list.");
        ElephantRunner.getInstance().getDistributedExecutorService().execute(analyticJob);
      } else {
        if (analyticJob != null) {
          MetricsController.markSkippedJob();
          logger.error("Drop the analytic job. Reason: reached the max retries for application id = ["
                  + analyticJob.getAppId() + "].");
        }
      }
    }
  }

  @Override
  public void waitInterval(long interval) {

    long nextRun = _lastRun + interval;
    long waitTime = nextRun - System.currentTimeMillis();

    if (waitTime <= 0) {
      return;
    }

    try {
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public long fetchCheckPoint() {

    CheckPoint checkPoint = Ebean.find(CheckPoint.class).where().eq("id", 1).findUnique();
    logger.info("lastTime: " + checkPoint.lastTime);
    return checkPoint.lastTime;
  }

  @Override
  public void updateCheckPoint() {

    String dml = "update check_point set last_time = :last_time where id = :id";
    SqlUpdate update = Ebean.createSqlUpdate(dml)
            .setParameter("last_time", _currentTime)
            .setParameter("id", 1);
    update.execute();
  }

  /**
   * Authenticate and update the token
   */
  private void updateAuthToken() {
    if (_currentTime - _tokenUpdatedTime > TOKEN_UPDATE_INTERVAL) {
      logger.info("AnalysisProvider updating its Authenticate Token...");
      _token = new AuthenticatedURL.Token();
      _authenticatedURL = new AuthenticatedURL();
      _tokenUpdatedTime = _currentTime;
    }
  }

  /**
   * Connect to url using token and return the JsonNode
   *
   * @param url The url to connect to
   * @return
   * @throws IOException Unable to get the stream
   * @throws AuthenticationException Authencation problem
   */
  private JsonNode readJsonNode(URL url)
      throws IOException, AuthenticationException {
    HttpURLConnection conn = _authenticatedURL.openConnection(url, _token);
    return _objectMapper.readTree(conn.getInputStream());
  }

  /**
   * Parse the returned json from Resource manager
   *
   * @param url The REST call
   * @return
   * @throws IOException
   * @throws AuthenticationException Problem authenticating to resource manager
   */
  private List<AnalyticJob> readApps(URL url) throws IOException, AuthenticationException{
    List<AnalyticJob> appList = new ArrayList<AnalyticJob>();

    JsonNode rootNode = readJsonNode(url);
    JsonNode apps = rootNode.path("apps").path("app");

    for (JsonNode app : apps) {
      String appId = app.get("id").getValueAsText();

      // When called first time after launch, hit the DB and avoid duplicated analytic jobs that have been analyzed
      // before.
      if (_lastTime > _fetchStartTime || (_lastTime == _fetchStartTime && AppResult.find.byId(appId) == null)) {
        String user = app.get("user").getValueAsText();
        String name = app.get("name").getValueAsText();
        String queueName = app.get("queue").getValueAsText();
        String trackingUrl = app.get("trackingUrl") != null? app.get("trackingUrl").getValueAsText() : null;
        long startTime = app.get("startedTime").getLongValue();
        long finishTime = app.get("finishedTime").getLongValue();

        ApplicationType type =
            ElephantContext.instance().getApplicationTypeForName(app.get("applicationType").getValueAsText());

        // If the application type is supported
        if (type != null) {
          AnalyticJob analyticJob = new AnalyticJob();
          analyticJob.setAppId(appId).setAppType(type).setUser(user).setName(name).setQueueName(queueName)
              .setTrackingUrl(trackingUrl).setStartTime(startTime).setFinishTime(finishTime);

          appList.add(analyticJob);
        }
      }
    }
    return appList;
  }
}
