package rpex.hadoop.mr.topn.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import rpex.hadoop.mr.topn.model.Limit;
import rpex.hadoop.mr.topn.model.TimeInterval;

import java.sql.Time;
import java.time.LocalDate;

/**
 * Parameters for executing top-n map reduce calculation.
 * This is simple and immutable data transfer object.
 */
@Getter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CalcTopN {
  private final Limit limit;
  private final TimeInterval timeInterval;

  private CalcTopN(final Limit limit, final TimeInterval timeInterval) {
    this.limit = limit;
    this.timeInterval = timeInterval;
  }

  public static CalcTopN of(int limit) {
    return new CalcTopN(Limit.of(limit), null);
  }

  @JsonCreator
  public static CalcTopN of(@JsonProperty("limit")int limit, @JsonProperty("timeInterval") TimeInterval timeInterval) {
    if (timeInterval == null) {
      return new CalcTopN(Limit.of(limit), null);
    } else {
      return new CalcTopN(Limit.of(limit), timeInterval);
    }
  }
}
