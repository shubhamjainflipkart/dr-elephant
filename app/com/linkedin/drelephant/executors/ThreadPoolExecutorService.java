package com.linkedin.drelephant.executors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.ElephantRunner;
import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.util.Utils;
import controllers.MetricsController;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadPoolExecutorService extends ThreadPoolExecutor implements IExecutorService {

    private static final Logger logger = Logger.getLogger(ThreadPoolExecutorService.class);

    private static final Configuration configuration = ElephantContext.instance().getGeneralConf();
    private static final int EXECUTOR_NUM = 5;                // The number of executor threads to analyse the jobs
    private static final String EXECUTOR_NUM_KEY = "drelephant.analysis.thread.count";

    private final Queue<AnalyticJob> _firstRetryQueue = new ConcurrentLinkedQueue<AnalyticJob>();
    private final ArrayList<AnalyticJob> _secondRetryQueue = new ArrayList<AnalyticJob>();

    private AtomicBoolean _running = new AtomicBoolean(true);
    private int _executorNum;

    public ThreadPoolExecutorService() {

        super(getExecutorNum(), getExecutorNum(), 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadFactoryBuilder().setNameFormat("dr-el-executor-thread-%d").build());
        _executorNum = getCorePoolSize();
    }

    private static int getExecutorNum() {
        return Utils.getNonNegativeInt(configuration, EXECUTOR_NUM_KEY, EXECUTOR_NUM);
    }

    /**
     * Add an AnalyticJob into retry list. Those jobs will be provided again via #fetchAnalyticJobs under
     * the generator's decision.
     *
     * @param promise The job to add
     */
    public void addIntoRetryQueue(AnalyticJob promise) {
        _firstRetryQueue.add(promise);
        int retryQueueSize = _firstRetryQueue.size();
        MetricsController.setRetryQueueSize(retryQueueSize);
        logger.info("Retry queue size is " + retryQueueSize);
    }

    /**
     * Add an AnalyticJob into the second retry list. This queue fetches jobs on greater intervals of time. Those jobs will be provided again via #fetchAnalyticJobs under
     * the generator's decision.
     *
     * @param promise The job to add
     */
    public void addIntoSecondRetryQueue(AnalyticJob promise) {
        _secondRetryQueue.add(promise.setTimeToSecondRetry());
        int secondRetryQueueSize = _secondRetryQueue.size();
        MetricsController.setSecondRetryQueueSize(secondRetryQueueSize);
        logger.info("Second Retry queue size is " + secondRetryQueueSize);
    }

    @Override
    public void startService() {

        long checkPoint = 0L;
        ElephantRunner elephantRunner = ElephantRunner.getInstance();

        logger.info("executor num is " + _executorNum);
        if (_executorNum < 1) {
            throw new RuntimeException("Must have at least 1 worker thread.");
        }

        while (_running.get() && !Thread.currentThread().isInterrupted()) {

            elephantRunner.getAnalyticJobGenerator().fetchAndExecuteJobs(checkPoint);
            checkPoint = elephantRunner.getAnalyticJobGenerator().getCheckPoint();

            int queueSize = getQueue().size();
            MetricsController.setQueueSize(queueSize);
            logger.info("Job queue size is " + queueSize);

            elephantRunner.getAnalyticJobGenerator().waitInterval(elephantRunner.getFetchInterval());
        }
    }

    @Override
    public List<AnalyticJob> getJobList() throws IOException, AuthenticationException {

        List<AnalyticJob> appList = ElephantRunner.getInstance().getAnalyticJobGenerator().fetchAnalyticJobs();
        while (!_firstRetryQueue.isEmpty()) {
            appList.add(_firstRetryQueue.poll());
        }

        Iterator iteratorSecondRetry = _secondRetryQueue.iterator();
        while (iteratorSecondRetry.hasNext()) {
            AnalyticJob job = (AnalyticJob) iteratorSecondRetry.next();
            if(job.readyForSecondRetry()) {
                appList.add(job);
                iteratorSecondRetry.remove();
            }
        }
        return appList;
    }

    @Override
    public void onPrimaryRetry(AnalyticJob analyticJob) {

        addIntoRetryQueue(analyticJob);
    }

    @Override
    public void onSecondaryRetry(AnalyticJob analyticJob) {

        addIntoSecondRetryQueue(analyticJob);
    }

    @Override
    public void execute(AnalyticJob analyticJob) {

        submit(new ExecutorJob(analyticJob));
    }

    @Override
    public void stopService() {
        _running.set(false);
        shutdownNow();
    }

    private class ExecutorJob implements Runnable {

        private AnalyticJob _analyticJob;

        public ExecutorJob(AnalyticJob analyticJob) {
            _analyticJob = analyticJob;
        }

        @Override
        public void run() {
            ElephantRunner.getInstance().getAnalyticJobGenerator().analyseJob(_analyticJob);
        }
    }
}