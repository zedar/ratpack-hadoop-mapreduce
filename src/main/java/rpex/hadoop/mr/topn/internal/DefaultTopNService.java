package rpex.hadoop.mr.topn.internal;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import mr.func.topn.TopNFunc;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ClassUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Blocking;
import ratpack.exec.Promise;
import ratpack.exec.Result;
import rpex.hadoop.mr.MapReduceService;
import rpex.hadoop.mr.topn.TopNService;
import rpex.hadoop.mr.topn.model.Limit;
import rpex.hadoop.mr.topn.model.TimeInterval;
import rpex.hadoop.mr.topn.model.UserActivityCounter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Executes top-n map-reduce.
 */
public class DefaultTopNService implements TopNService {
  private final Logger LOGGER = LoggerFactory.getLogger(DefaultTopNService.class);

  private final MapReduceService mapReduceService;

  public DefaultTopNService(MapReduceService mapReduceService) {
    this.mapReduceService = mapReduceService;
  }

  @Override
  public Promise<Result<List<UserActivityCounter>>> apply(Limit limit, TimeInterval timeInterval, String inputFS, String outputFS) {
    return mapReduceService.provide("top-n-users")
      .onNull(() -> Result.error(new IllegalArgumentException("MAPREDUCE_JOB_CREATION_FAILED")))
      .flatMap(mrJob -> Blocking.get(() -> {
        LOGGER.debug("STARTING job execution, limit={}", limit.getValue());
        mrJob.getJob().setJarByClass(TopNFunc.class);
        LOGGER.debug("FUNC JAR: {}", ClassUtil.findContainingJar(TopNFunc.class));
        mrJob.getJob().setMapperClass(TopNFunc.Mapper.class);
        mrJob.getJob().setCombinerClass(TopNFunc.Combiner.class);
        mrJob.getJob().setReducerClass(TopNFunc.Reducer.class);

        mrJob.getJob().setOutputKeyClass(Text.class);
        mrJob.getJob().setOutputValueClass(IntWritable.class);

        Path inputPath = mrJob.getJobPath(Strings.isNullOrEmpty(inputFS) ? inputFS : "input"),
              outputPath = mrJob.getJobPath(Strings.isNullOrEmpty(outputFS) ? outputFS : "output");

        // delete output path
        mrJob.getFileSystem().delete(outputPath, true);

        FileInputFormat.addInputPath(mrJob.getJob(), inputPath);
        FileOutputFormat.setOutputPath(mrJob.getJob(), outputPath);

        // set custom parameters
        mrJob.getJob().getConfiguration().set("limit", limit.getValue().toString());
        mrJob.getJob().getConfiguration().set("dateFrom", timeInterval.getDateFrom().toString());
        mrJob.getJob().getConfiguration().set("dateTo", timeInterval.getDateTo().toString());

        if (!mrJob.getJob().waitForCompletion(false)) {
          return Result.error(new RuntimeException("MAPREDUCE_TIMEOUT"));
        }

        FileStatus[] status = mrJob.getFileSystem().listStatus(outputPath, new PathFilter() {
          @Override
          public boolean accept(Path path) {
            return path.getName().startsWith("part");
          }
        });

        ArrayList<UserActivityCounter> userActivityCounters = Lists.newArrayList();
        for (int i = 0; i < status.length; i++) {
          FSDataInputStream is = mrJob.getFileSystem().open(status[i].getPath());
          BufferedReader br = new BufferedReader(new InputStreamReader(is));
          String line = br.readLine();
          while(line != null) {
            String[] parts = line.split("\\s+");
            if (parts != null && parts.length >= 2) {
              LOGGER.debug("RESULT: {} {}", parts[0], parts[1]);
              userActivityCounters.add(new UserActivityCounter(parts[0], Integer.valueOf(parts[1])));
            }
            line = br.readLine();
          }
          is.close();
        }
        LOGGER.debug("END OF job execution");
        return Result.success(ImmutableList.copyOf(userActivityCounters));
      }));
  }
}
