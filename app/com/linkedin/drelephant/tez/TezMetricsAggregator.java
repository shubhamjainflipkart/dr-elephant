package com.linkedin.drelephant.tez;

import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.analysis.HadoopMetricsAggregator;
import com.linkedin.drelephant.analysis.HadoopAggregatedData;
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData;
import com.linkedin.drelephant.tez.data.TezDAGApplicationData;
import com.linkedin.drelephant.tez.data.TezVertexTaskData;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * 
 * Calculates the tez metrics and aggregates them.  
 * 
 */
public class TezMetricsAggregator implements HadoopMetricsAggregator {

  private static final Logger logger = Logger.getLogger(TezMetricsAggregator.class);
  private static final String TEZ_CONTAINER_CONFIG = "tez.am.resource.memory.mb";
  private static final long CONTAINER_MEMORY_DEFAULT_MBYTES = 2048L;

  private HadoopAggregatedData _hadoopAggregatedData = null;
 
  private AggregatorConfigurationData _aggregatorConfigurationData;

  public TezMetricsAggregator(AggregatorConfigurationData _aggregatorConfigurationData) {
    this._aggregatorConfigurationData = _aggregatorConfigurationData;
    _hadoopAggregatedData = new HadoopAggregatedData();
  }

  @Override
  public void aggregate(HadoopApplicationData hadoopData) {

    TezDAGApplicationData data = (TezDAGApplicationData) hadoopData;

    long tezContainerSize = getTezContainerSize(data);

  

    TaskLevelAggregatedMetrics  tezTasks = new  TaskLevelAggregatedMetrics(data.getTezDAGData(), tezContainerSize,0l);

    _hadoopAggregatedData.setResourceUsed(tezTasks.getResourceUsed());
    _hadoopAggregatedData.setTotalDelay(tezTasks.getDelay() );
    _hadoopAggregatedData.setResourceWasted(tezTasks.getResourceWasted() );
  }

  @Override
  public HadoopAggregatedData getResult() {
    return _hadoopAggregatedData;
  }

  private long getTezContainerSize(HadoopApplicationData data) {
    try {
      long value = Long.parseLong(data.getConf().getProperty(TEZ_CONTAINER_CONFIG));
      return (value < 0) ? CONTAINER_MEMORY_DEFAULT_MBYTES : value;
    } catch ( NumberFormatException ex) {
      return CONTAINER_MEMORY_DEFAULT_MBYTES;
    }
  }

 
}
