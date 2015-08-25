package rpex.hadoop.mr;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 * Wrapper for hadoop's mapreduce {@link org.apache.hadoop.mapreduce.Job} and {@link org.apache.hadoop.fs.FileSystem}.
 */
@Getter
@AllArgsConstructor
public class MapReduceJob {
  private final MapReduceConfig config;
  private final Job job;
  private final FileSystem fileSystem;

  /**
   * Hadoop file system path calculated out of {@link MapReduceConfig#getFileSystemAddress()},
   * {@link MapReduceConfig#getUser()} and input {@code fsName}
   * @param fsName a name of file or directory on hadoop file system
   * @return the path to hadoop file system
   */
  public Path getJobPath(String fsName) {
    StringBuilder b = new StringBuilder();
    b.append(config.getFileSystemAddress())
      .append("/user/")
      .append(config.getUser())
      .append("/")
      .append(fsName);
    return new Path(b.toString());
  }
}
