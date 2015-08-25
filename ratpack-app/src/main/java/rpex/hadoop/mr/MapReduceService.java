package rpex.hadoop.mr;

import ratpack.exec.Promise;

/**
 * Infrastructure for executing map-reduce jobs.
 */
public interface MapReduceService {
  /**
   * Provides execution infrastructure for hadoop's map-reduce based on the {@link MapReduceConfig}
   * @param jobName job name
   * @return the promise for map-reduce execution infrastructure
   */
  Promise<MapReduceJob> provide(String jobName);
}
