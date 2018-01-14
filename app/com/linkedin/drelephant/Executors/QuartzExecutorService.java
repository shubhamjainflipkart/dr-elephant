package com.linkedin.drelephant.Executors;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.ElephantRunner;
import com.linkedin.drelephant.analysis.AnalyticJob;
import org.apache.log4j.Logger;
import org.quartz.*;
import org.apache.hadoop.conf.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

public class QuartzExecutorService implements IExecutorService {

    private static final Logger logger = Logger.getLogger(QuartzExecutorService.class);

    private static final String SKIP_UPDATE_CHECK = "org.quartz.scheduler.skipUpdateCheck";
    private static final String INSTANCE_NAME = "org.quartz.scheduler.instanceName";
    private static final String INSTANCE_ID = "org.quartz.scheduler.instanceId";
    private static final String JOB_FACTORY_CLASS = "org.quartz.scheduler.jobFactory.class";
    private static final String JOB_STORE_CLASS = "org.quartz.jobStore.class";
    private static final String DRIVER_DELEGATE_CLASS = "org.quartz.jobStore.driverDelegateClass";
    private static final String DATA_SOURCE = "org.quartz.jobStore.dataSource";
    private static final String TABLE_PREFIX = "org.quartz.jobStore.tablePrefix";
    private static final String IS_CLUSTERED = "org.quartz.jobStore.isClustered";
    private static final String THREAD_POOL_CLASS = "org.quartz.threadPool.class";
    private static final String THREAD_POOL_COUNT = "org.quartz.threadPool.threadCount";
    private static final String DATA_SOURCE_DRIVER = "org.quartz.dataSource.quartzDataSource.driver";
    private static final String DATA_SOURCE_URL = "org.quartz.dataSource.quartzDataSource.URL";
    private static final String DATA_SOURCE_USER = "org.quartz.dataSource.quartzDataSource.user";
    private static final String DATA_SOURCE_PASSWORD = "org.quartz.dataSource.quartzDataSource.password";
    private static final String DATA_SOURCE_MAX_CONNECTIONS = "org.quartz.dataSource.quartzDataSource.maxConnections";

    private String instanceName;
    private String threadPoolCount;
    private String dataSourceUrl;
    private String dataSourceUser;
    private String dataSourcePassword;
    private String dataSourceMaxConnections;
    private Scheduler scheduler;


    public QuartzExecutorService() {
        try {
            loadQuartzConfiguration();
            scheduler = new org.quartz.impl.StdSchedulerFactory(buildProps()).getScheduler();
            scheduler.start();
        } catch (SchedulerException e) {
            throw new RuntimeException("Error while setting up scheduler : ", e);
        }
    }

     private void loadQuartzConfiguration() {

        Configuration configuration = ElephantContext.instance().getQuartzConf();
        instanceName = configuration.get(INSTANCE_NAME);
        threadPoolCount = configuration.get(THREAD_POOL_COUNT);
        dataSourceUrl = configuration.get(DATA_SOURCE_URL);
        dataSourceUser = configuration.get(DATA_SOURCE_USER);
        dataSourcePassword = configuration.get(DATA_SOURCE_PASSWORD);
        dataSourceMaxConnections = configuration.get(DATA_SOURCE_MAX_CONNECTIONS);
    }

    private String getInetAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Not able to set instance id for this scheduler", e);
        }
    }

    private Properties buildProps() {

        final Properties properties = new Properties();

        properties.put(JOB_FACTORY_CLASS, "org.quartz.simpl.SimpleJobFactory");
        properties.put(JOB_STORE_CLASS, "org.quartz.impl.jdbcjobstore.JobStoreTX");
        properties.put(DRIVER_DELEGATE_CLASS, "org.quartz.impl.jdbcjobstore.StdJDBCDelegate");
        properties.put(DATA_SOURCE, "quartzDataSource");
        properties.put(THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
        properties.put(DATA_SOURCE_DRIVER, "com.mysql.jdbc.Driver");
        properties.put(TABLE_PREFIX, "QRTZ_");
        properties.put(SKIP_UPDATE_CHECK, String.valueOf(true));
        properties.put(IS_CLUSTERED, String.valueOf(true));
        properties.put(INSTANCE_ID, getInetAddress());
        properties.put(INSTANCE_NAME, instanceName);
        properties.put(THREAD_POOL_COUNT, threadPoolCount);
        properties.put(DATA_SOURCE_URL, dataSourceUrl);
        properties.put(DATA_SOURCE_USER, dataSourceUser);
        properties.put(DATA_SOURCE_PASSWORD, dataSourcePassword);
        properties.put(DATA_SOURCE_MAX_CONNECTIONS, dataSourceMaxConnections);
        return properties;
    }

    @Override
    public void startService() {
        try {
            JobDetail job = JobBuilder.newJob(QuartzExecutorService.SchedulerJob.class)
                    .withIdentity(constructJobKey("schedulerJob", SchedulerJob.class.getName()))
                    .build();
            Trigger trigger = TriggerBuilder.newTrigger().withIdentity("MainTrigger")
                    .withSchedule(
                            simpleSchedule()
                                    .withIntervalInMilliseconds(ElephantRunner.getInstance().getFetchInterval())
                                    .repeatForever()
                                    .withMisfireHandlingInstructionFireNow()
                    ).startNow()
                    .build();
            scheduler.scheduleJob(job, trigger);
        } catch (ObjectAlreadyExistsException e) {
            logger.info("job already exist");
        } catch (SchedulerException e) {
            throw new RuntimeException("Error while setting up scheduler : ", e);
        }
    }

    @Override
    public void execute(AnalyticJob analyticJob) {
        try {
            JobDetail job = JobBuilder.newJob(QuartzExecutorService.ExecutorJob.class)
                    .withIdentity(constructJobKey(analyticJob.getAppId(), ExecutorJob.class.getName()))
                    .usingJobData(constructJobDataMap("analyticJob", analyticJob))
                    .build();
            Trigger trigger = TriggerBuilder.newTrigger().withIdentity("simpleTrigger: " + analyticJob.getAppId())
                    .startNow()
                    .withSchedule(
                            simpleSchedule()
                                    .withMisfireHandlingInstructionFireNow()
                    ).build();
            scheduler.scheduleJob(job, trigger);
        } catch (ObjectAlreadyExistsException e) {
            System.out.println("job already exist with app_id: "+ analyticJob.getAppId());
        } catch (SchedulerException e) {
            throw new RuntimeException("Error while setting up scheduler : ", e);
        }
    }

    public void stopService() {
        try {
            scheduler.shutdown();
        } catch (SchedulerException e) {
            logger.debug("Cannot shutdown", e);
        }
    }

    private JobDataMap constructJobDataMap(String key, AnalyticJob analyticJob) {
        final JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(key, analyticJob);
        return jobDataMap;
    }

    private JobKey constructJobKey(String jobName, String jobGroup) {
        return new JobKey(jobName, jobGroup);
    }

    @DisallowConcurrentExecution
    public static class SchedulerJob implements Job {

        private long checkPoint;

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            checkPoint = ElephantRunner.getInstance().getAnalyticJobGenerator().fetchCheckPoint();
            ElephantRunner.getInstance().getAnalyticJobGenerator().fetchAndExecuteJobs(checkPoint);
        }
    }

    public static class ExecutorJob implements Job {

        private AnalyticJob analyticJob;

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            analyticJob = (AnalyticJob) context.getJobDetail().getJobDataMap().get("analyticJob");
            ElephantRunner.getInstance().getAnalyticJobGenerator().jobAnalysis(analyticJob);
        }
    }
}
