package com.linkedin.drelephant.executors;

import com.linkedin.drelephant.analysis.AnalyticJob;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import java.io.IOException;
import java.util.List;

public interface IExecutorService {

    void startService();

    void onPrimaryRetry(AnalyticJob analyticJob);

    void onSecondaryRetry(AnalyticJob analyticJob);

    List<AnalyticJob> getJobList() throws IOException, AuthenticationException;

    void execute(AnalyticJob analyticJob);

    void stopService();

}