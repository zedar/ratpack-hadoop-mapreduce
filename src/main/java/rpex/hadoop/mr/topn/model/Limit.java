package rpex.hadoop.mr.topn.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Optional;

/**
 * Limits the number of unique entities queried on the data file.
 * <p>
 * Immutable value object.
 */
@Getter
@ToString
@EqualsAndHashCode
public class Limit {
  private final Integer value;

  private Limit(int value) {
    this.value = value;
  }

  public static Limit of(int value) {
    if (!inRange(value, 1, Integer.MAX_VALUE)) {
      return null;
    } else {
      return new Limit(value);
    }
  }

  private static boolean inRange(int value, int min, int max) {
    return value >= min && value <= max;
  }
}
