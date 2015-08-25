package rpex.hadoop.mr.internal;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Promise;
import rpex.hadoop.Main;
import rpex.hadoop.mr.MapReduceConfig;
import rpex.hadoop.mr.MapReduceJob;
import rpex.hadoop.mr.MapReduceService;

/**
 * Provides infrastructure for Hadoop's map-reduce execution.
 */
public class DefaultMapReduceService implements MapReduceService {
  private final Logger LOGGER = LoggerFactory.getLogger(DefaultMapReduceService.class);

  private final MapReduceConfig config;

  public DefaultMapReduceService(final MapReduceConfig config) {
    this.config = config;
  }

  @Override
  public Promise<MapReduceJob> provide(String jobName) {
    if (Strings.isNullOrEmpty(jobName)) {
      return Promise.value(null);
    }
    return Promise.of(f -> {
      LOGGER.debug("STARTING providing MapReduceJob, config: {}", config.toString());
      Configuration configuration = new Configuration();
      if (!Strings.isNullOrEmpty(config.getUser())) {
        configuration.set("hadoop.job.ugi", config.getUser());
      }
      LOGGER.debug("YARN RM ADDRESS: {}", config.getYarnRMAddress());
      LOGGER.debug("YARN RM SCHEDULER ADDRESS: {}", config.getYarnRMSchedulerAddress());
      LOGGER.debug("HDFS: {}", config.getFileSystemAddress());

      configuration.set("yarn.resourcemanager.address", config.getYarnRMAddress());
      configuration.set("yarn.resourcemanager.scheduler.address", config.getYarnRMSchedulerAddress());
      configuration.set("mapreduce.framework.name", config.getMapReduceType());
      configuration.set("fs.default.name", config.getFileSystemAddress());

      Job job = Job.getInstance(configuration, jobName);

      f.success(new MapReduceJob(config, job, FileSystem.get(configuration)));
      LOGGER.debug("END OF providing MapReduceJob");
    });
  }
}
