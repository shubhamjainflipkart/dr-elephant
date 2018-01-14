package com.linkedin.drelephant.Executors;

import com.linkedin.drelephant.analysis.AnalyticJob;

public interface IExecutorService {

    void startService();

    void execute(AnalyticJob analyticJob);

    void stopService();

}
