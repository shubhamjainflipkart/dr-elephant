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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;


/**
 * Provides AnalyticJobs that will yield to analysis results later. This class basically generates to-dos that could be
 * executed later.
 */
public interface AnalyticJobGenerator {

  /**
   * Configures the provider instance
   *
   * @param configuration The Hadoop configuration object
   * @throws Exception
   */
  public void configure(Configuration configuration)
      throws IOException;

  /**
   * Configures the resource manager addresses considering HA
   */
  public void updateResourceManagerAddresses();

  /**
   * Fetches Analytic jobs since checkpoint, executes the executor service and updates checkpoint.
   * @param checkPoint time till which jobs have been analysed.
   */
  public void fetchAndExecuteJobs(long checkPoint);

  /**
   * Do analysis of each AnalyticJob
   * @param analyticJob Analytic job to be analysed
   */
  public void analyseJob(AnalyticJob analyticJob);

  public void waitInterval(long interval);

  /**
   * Updates the checkpoint till the time jobs have been analysed
   */
  public void updateCheckPoint();

  /**
   * fetches the checkpoint till the time jobs have been analysed
   * @return checkpoint
   */
  public long getCheckPoint();
}
