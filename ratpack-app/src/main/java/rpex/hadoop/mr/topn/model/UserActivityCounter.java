package rpex.hadoop.mr.topn.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * User activity counter. Counts all requests sent by a user.
 */
@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class UserActivityCounter {
  private final String username;
  private final Integer counter;
}
