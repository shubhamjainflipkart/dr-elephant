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

package com.linkedin.drelephant.tez;

import com.linkedin.drelephant.tez.data.TezCounterData;
import com.linkedin.drelephant.tez.data.TezDAGApplicationData;
import com.linkedin.drelephant.tez.data.TezDAGData;
import com.linkedin.drelephant.tez.data.TezVertexData;
import com.linkedin.drelephant.tez.data.TezVertexTaskData;

import org.junit.Assert;
import org.junit.Test;

public class TestTaskLevelAggregatedMetrics {

    @Test
    public void testZeroTasks() {
    	TezDAGData taskData[] = {};
        TaskLevelAggregatedMetrics taskMetrics = new TaskLevelAggregatedMetrics(taskData, 0, 0);
        Assert.assertEquals(taskMetrics.getDelay(), 0);
        Assert.assertEquals(taskMetrics.getResourceUsed(), 0);
        Assert.assertEquals(taskMetrics.getResourceWasted(), 0);
    }

    @Test
    public void testNullTaskArray() {
        TaskLevelAggregatedMetrics taskMetrics = new TaskLevelAggregatedMetrics(null, 0, 0);
        Assert.assertEquals(taskMetrics.getDelay(), 0);
        Assert.assertEquals(taskMetrics.getResourceUsed(), 0);
        Assert.assertEquals(taskMetrics.getResourceWasted(), 0);
    }

    @Test
    public void testTaskLevelData() {
    	TezVertexTaskData taskData[] = new TezVertexTaskData[3];
    	//TezDAGData tezDAGData[] = new TezDAGData[3];
        TezCounterData counterData = new TezCounterData();
        counterData.set(TezCounterData.CounterName.PHYSICAL_MEMORY_BYTES, 655577088L);
        counterData.set(TezCounterData.CounterName.VIRTUAL_MEMORY_BYTES, 3051589632L);
        long time[] = {0,0,0,1464218501117L, 1464218534148L};
        taskData[0] = new TezVertexTaskData("task", "id");
        taskData[0].setTime(time);
        taskData[0].setCounter(counterData);
        taskData[1] = new TezVertexTaskData("task", "id");
        taskData[1].setTime(new long[5]);
        taskData[1].setCounter(counterData);
        // Non-sampled task, which does not contain time and counter data
        taskData[2] = new TezVertexTaskData("task", "id");
        //taskData[2].setTime(new long[5]);
        //taskData[2].setCounter(counterData);
        TezDAGData tezDags[] = new TezDAGData[1];
        TezDAGData tezDAGData = new TezDAGData(counterData);
        TezVertexData tezVertexes[] = new TezVertexData[1];
        TezVertexData tezVertexData = new TezVertexData("new vertex");
        tezVertexData.setCounter(counterData);
        tezVertexes[0]=tezVertexData;
        tezVertexData.setMapperData(taskData);
        tezDags[0]=tezDAGData;
        tezDAGData.setVertexData(tezVertexes);

        TezDAGApplicationData data = new TezDAGApplicationData();
        data.setCounters(counterData).setTezDAGData(tezDags);
        TaskLevelAggregatedMetrics taskMetrics = new TaskLevelAggregatedMetrics(tezDags, 4096L, 1463218501117L);
        Assert.assertEquals(taskMetrics.getDelay(), 0L);
        Assert.assertEquals(taskMetrics.getResourceUsed(), 0L);
        Assert.assertEquals(taskMetrics.getResourceWasted(), 0L);
    }
}
