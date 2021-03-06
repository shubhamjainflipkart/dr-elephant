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

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;

import java.io.IOException;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.tez.data.TezCounterData;
import com.linkedin.drelephant.tez.data.TezDAGApplicationData;
import com.linkedin.drelephant.tez.data.TezDAGData;
import com.linkedin.drelephant.tez.data.TezVertexData;
import com.linkedin.drelephant.tez.data.TezVertexTaskData;
import com.linkedin.drelephant.math.Statistics;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;


public class ShuffleSortHeuristicTest extends TestCase {

  private static Map<String, String> paramsMap = new HashMap<String, String>();
  private static Heuristic _heuristic = new ShuffleSortHeuristic(new HeuristicConfigurationData("test_heuristic",
      "test_class", "test_view", new ApplicationType("test_apptype"), paramsMap));

  private static final int NUMTASKS = 100;
  private static final long MINUTE_IN_MS = Statistics.MINUTE_IN_MS;;

  public void testLongShuffleCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(30 * MINUTE_IN_MS, 0, 5 * MINUTE_IN_MS));
  }

  public void testLongShuffleSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(30 * MINUTE_IN_MS, 0, 10 * MINUTE_IN_MS));
  }

  public void testLongShuffleModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(30 * MINUTE_IN_MS, 0, 20 * MINUTE_IN_MS));
  }

  public void testLongShuffleLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(30 * MINUTE_IN_MS, 0, 40 * MINUTE_IN_MS));
  }

  public void testLongShuffleNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(30 * MINUTE_IN_MS, 0, 80 * MINUTE_IN_MS));
  }

  public void testLongSortCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(0, 30 * MINUTE_IN_MS, 5 * MINUTE_IN_MS));
  }

  public void testLongSortSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(0, 30 * MINUTE_IN_MS, 10 * MINUTE_IN_MS));
  }

  public void testLongSortModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(0, 30 * MINUTE_IN_MS, 20 * MINUTE_IN_MS));
  }

  public void testLongSortLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(0, 30 * MINUTE_IN_MS, 40 * MINUTE_IN_MS));
  }

  public void testLongSortNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(0, 30 * MINUTE_IN_MS, 80 * MINUTE_IN_MS));
  }

  public void testShortShuffle() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(MINUTE_IN_MS / 2, 0, MINUTE_IN_MS / 2));
  }

  public void testShortSort() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(0, MINUTE_IN_MS / 2, MINUTE_IN_MS / 2));
  }

  private Severity analyzeJob(long shuffleTimeMs, long sortTimeMs, long reduceTimeMs) throws IOException {
    TezCounterData dummyCounter = new TezCounterData();
    TezVertexTaskData[] reducers = new TezVertexTaskData[NUMTASKS + 1];

    int i = 0;
    for (; i < NUMTASKS; i++) {
      reducers[i] = new TezVertexTaskData("task-id-"+i, "task-attempt-id-"+i);
      reducers[i].setTime(
          new long[] { shuffleTimeMs + sortTimeMs + reduceTimeMs, shuffleTimeMs, sortTimeMs, 0, 0});
          reducers[i].setCounter(dummyCounter);
    }
    // Non-sampled task, which does not contain time and counter data
    reducers[i] = new TezVertexTaskData("task-id-"+i, "task-attempt-id-"+i);
    TezDAGData tezDags[] = new TezDAGData[1];
    TezDAGData tezDAGData = new TezDAGData(dummyCounter);
    TezVertexData tezVertexes[] = new TezVertexData[1];
    TezVertexData tezVertexData = new TezVertexData("new vertex");
    tezVertexes[0]=tezVertexData;
    tezVertexData.setReducerData(reducers);
    tezDags[0]=tezDAGData;
    tezDAGData.setVertexData(tezVertexes);

    TezDAGApplicationData data = new TezDAGApplicationData();
    data.setCounters(dummyCounter).setTezDAGData(tezDags);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }

}
