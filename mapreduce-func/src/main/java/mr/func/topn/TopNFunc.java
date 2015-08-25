package mr.func.topn;

import com.google.common.base.Strings;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Map-reduce functions for counting users activity out of the access logs.
 */
public class TopNFunc {
  /**
   * As the Map operation is parallelized the input file set is first split to several pieces.
   * <p>
   * Mapper is run for the the split - the input file is split into several splits (of size 64MB).
   * Mapper works with a record - each line from the split is a record
   */
  public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Mapper.class);

    private static final String TIME_AND_USER_REGEX = "^(\\[)([0-9]{2}\\/[A-Za-z]{3}\\/[0-9]{4})(.)+(username=')([A-Za-z0-9]{8}+)(')";
    private static final Pattern TIME_AND_USER_PATTERN = Pattern.compile(TIME_AND_USER_REGEX);

    private static final String USER_REGEX = "(username=')([A-Za-z0-9]{8}+)(')";
    private static final Pattern USER_PATTERN = Pattern.compile(USER_REGEX);

    private static final IntWritable one = new IntWritable(1);
    private Text usernameWord = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String df = context.getConfiguration().get("dateFrom");
      String dt = context.getConfiguration().get("dateTo");
      if (!Strings.isNullOrEmpty(df) && !Strings.isNullOrEmpty(dt)) {
        Matcher matcher = TIME_AND_USER_PATTERN.matcher(value.toString());
        if (matcher.find()) {
          String d = matcher.group(2);
          LocalDate dateFrom = LocalDate.parse(df), dateTo = LocalDate.parse(dt);
          LocalDate date = LocalDate.parse(d, DateTimeFormatter.ofPattern("dd/MMM/yyyy").withLocale(Locale.ENGLISH));
          if (date.isBefore(dateFrom) || date.isAfter(dateTo)) {
            return;
          }
          String username = matcher.group(5);
          usernameWord.set(username);
          context.write(usernameWord, one);
        }
      } else {
        Matcher matcher = USER_PATTERN.matcher(value.toString());
        if (matcher.find()) {
          String username = matcher.group(2);
          usernameWord.set(username);
          context.write(usernameWord, one);
        }
      }
    }
  }

  /**
   * Combines partitions (key-value pairs that are output of mapper) into summed pairs key-num of actions.
   * <p>
   * IMPORTANT: this is done for mapper only, not for all partitions.
   * <p>
   * When the map operation outputs its pairs they are already available in memory.
   * <p>
   * If a combiner is used then the map key-value pairs are not immediately written to the output.
   * Instead they will be collected in lists, one list per each key value.
   */
  public static class Combiner extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Combiner.class);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

  /**
   * Combines partitions globally, for all mappers.
   */
  public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Reducer.class);

    Map<String, Integer> counters = new ConcurrentHashMap<String, Integer>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      counters.put(key.toString(), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      // get parameters
      String limit = context.getConfiguration().get("limit");
      Integer n = Strings.isNullOrEmpty(limit) ? 10 : Integer.valueOf(limit);

      // sort the users by counters
      List<String> keys = new ArrayList<String>(counters.keySet());
      try {
        keys
          .stream()
          .sorted((k1, k2) -> counters.get(k2).compareTo(counters.get(k1)))
          .limit(n)
          .collect(Collectors.toList())
          .forEach(k -> {
            try {
              context.write(new Text(k), new IntWritable(counters.get(k)));
            } catch (Exception ex) {
              throw new RuntimeException(ex);
            }
          });
      } catch (RuntimeException ex) {
        if (ex.getCause() != null) {
          if (ex.getCause() instanceof IOException) {
            throw (IOException)ex.getCause();
          } else if (ex.getCause() instanceof InterruptedException) {
            throw (InterruptedException)ex.getCause();
          }
        } else {
          throw ex;
        }
      }
    }
  }
}
