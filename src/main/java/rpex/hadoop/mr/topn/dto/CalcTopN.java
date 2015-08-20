package rpex.hadoop.mr.topn.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import rpex.hadoop.mr.topn.model.Limit;
import rpex.hadoop.mr.topn.model.TimeInterval;

import java.time.LocalDate;

/**
 * Parameters for executing top-n map reduce calculation.
 * This is simple and immutable data transfer object.
 */
@Getter
@ToString
public class CalcTopN {
  private final Limit limit;

  private CalcTopN(final Limit limit) {
    this.limit = limit;
  }

  @JsonCreator
  public static CalcTopN of(@JsonProperty("limit")int limit) {
    return new CalcTopN(Limit.of(limit));
  }
}
