package rpex.hadoop.mr.topn;

import ratpack.exec.Promise;
import ratpack.exec.Result;
import rpex.hadoop.mr.topn.model.Limit;
import rpex.hadoop.mr.topn.model.TimeInterval;
import rpex.hadoop.mr.topn.model.UserActivityCounter;

import java.util.List;

/**
 * Runner for the top-n mapreduce job.
 */
public interface TopNService {
  /**
   * Executes map-reduce for calculating top-n users by their activity.
   * <p>
   * If {@code timeInterval} is provided calculation is executed between {@code dateFrom} and {@code dateTo}
   * @param limit a limit for the number of the most active users
   * @param timeInterval a time interval for looking for the most active users
   * @param inputFS a hadoop file system where user activity logs are stored
   * @param outputFS a hadoop file system where calculation results are stored
   * @return the promise for the result of top-n map-reduce calculation
   */
  Promise<Result<List<UserActivityCounter>>> apply(Limit limit, TimeInterval timeInterval, String inputFS, String outputFS);
}
