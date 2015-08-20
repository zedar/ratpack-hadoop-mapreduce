package rpex.hadoop.mr.topn.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.time.LocalDate;

/**
 * Defines time interval between two dates: {@code dateFrom} and {@code dateTo}.
 * <p>
 * Immutable value object.
 */
@Getter
@ToString
@EqualsAndHashCode
public class TimeInterval {
  private final LocalDate dateFrom;
  private final LocalDate dateTo;

  private TimeInterval(LocalDate dateFrom, LocalDate dateTo) {
    this.dateFrom = dateFrom;
    this.dateTo = dateTo;
  }

  /**
   * Verifies if time interval contains the date.
   * @param date a date to check if belongs to time interval
   * @return true if date is within time interval
   */
  public boolean contains(LocalDate date) {
    if (date == null) {
      return false;
    }
    if (dateFrom.isAfter(date) || dateTo.isBefore(date)) {
      return false;
    }
    return true;
  }

  /**
   * Creates time interval with all checkings.
   * @param dateFrom a date from which time interval starts
   * @param dateTo a date when time interval ends
   * @return the time interval
   */
  public static TimeInterval of(LocalDate dateFrom, LocalDate dateTo) {
    if (dateFrom == null || dateTo == null || dateFrom.isEqual(dateTo) || dateFrom.isAfter(dateTo)) {
      return null;
    }
    return new TimeInterval(dateFrom, dateTo);
  }
}
