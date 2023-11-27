package io.tarantool.driver.mappers.converters;

import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.time.temporal.ChronoUnit.WEEKS;
import static java.time.temporal.ChronoUnit.YEARS;

import io.tarantool.driver.utils.Assert;

/**
 * @author Artyom Dubinin
 * <p>
 * TODO: implement Temporal to add or substract Interval instances
 */
public class Interval implements TemporalAmount {
    // github.com/tarantool/tarantool/blob/ff57f990f359f6d7866c1947174d8ba0e97b1ea6/src/lua/datetime.lua#L112-L146
    private static final ThreadLocal<StringBuilder> threadLocalStringBuilder;
    private static final long SECS_PER_DAY = 86400;
    private static final long MIN_DATE_YEAR = -5879610;
    private static final long MIN_DATE_MONTH = 6;
    private static final long MIN_DATE_DAY = 22;
    private static final long MAX_DATE_YEAR = 5879611;
    private static final long MAX_DATE_MONTH = 7;
    private static final long MAX_DATE_DAY = 11;
    private static final long AVERAGE_DAYS_YEAR = 365;
    private static final long AVERAGE_WEEK_YEAR = AVERAGE_DAYS_YEAR / 7;
    private static final String OUT_OF_ALLOWED_RANGE = "is out of allowed range [-";
    private static final String YEARS_BOUND_FORMAT;
    private static final String MOTH_BOUND_FORMAT;
    private static final String WEEK_BOUND_FORMAT;
    private static final String DAY_BOUND_FORMAT;
    private static final String HOUR_BOUND_FORMAT;
    private static final String MIN_BOUND_FORMAT;
    private static final String SEC_BOUND_FORMAT;
    private static final String NSEC_BOUND_FORMAT;
    private static final String VALUE_BOUND_FORMAT;

    private static final List<TemporalUnit> SUPPORTED_UNITS = Collections.unmodifiableList(Arrays.<TemporalUnit>asList(
        YEARS,
        MONTHS,
        WEEKS,
        DAYS,
        HOURS,
        MINUTES,
        SECONDS,
        NANOS));

    public static final long MAX_NSEC_RANGE = Integer.MAX_VALUE;
    public static final long MAX_YEAR_RANGE = MAX_DATE_YEAR - MIN_DATE_YEAR;
    public static final long MAX_MONTH_RANGE = MAX_YEAR_RANGE * 12;
    public static final long MAX_DAY_RANGE = MAX_YEAR_RANGE * AVERAGE_DAYS_YEAR;
    public static final long MAX_HOUR_RANGE = MAX_DAY_RANGE * 24;
    public static final long MAX_MIN_RANGE = MAX_HOUR_RANGE * 60;
    public static final long MAX_SEC_RANGE = MAX_DAY_RANGE * SECS_PER_DAY;
    public static final long MAX_WEEK_RANGE = MAX_YEAR_RANGE * AVERAGE_WEEK_YEAR;

    private long year;
    private long month;
    private long week;
    private long day;
    private long hour;
    private long min;
    private long sec;
    private long nsec;
    private Adjust adjust = Adjust.NoneAdjust;

    static {
        threadLocalStringBuilder = ThreadLocal.withInitial(StringBuilder::new);
        VALUE_BOUND_FORMAT = "Value ";
        YEARS_BOUND_FORMAT = " of year " + OUT_OF_ALLOWED_RANGE + MAX_YEAR_RANGE + ", " + MAX_YEAR_RANGE + "]";
        MOTH_BOUND_FORMAT = " of month " + OUT_OF_ALLOWED_RANGE + MAX_MONTH_RANGE + ", " + MAX_MONTH_RANGE + "]";
        WEEK_BOUND_FORMAT = " of week " + OUT_OF_ALLOWED_RANGE + MAX_WEEK_RANGE + ", " + MAX_WEEK_RANGE + "]";
        DAY_BOUND_FORMAT = " of day " + OUT_OF_ALLOWED_RANGE + MAX_DAY_RANGE + ", " + MAX_DAY_RANGE + "]";
        HOUR_BOUND_FORMAT = " of hour " + OUT_OF_ALLOWED_RANGE + MAX_HOUR_RANGE + ", " + MAX_HOUR_RANGE + "]";
        MIN_BOUND_FORMAT = " of min " + OUT_OF_ALLOWED_RANGE + MAX_MIN_RANGE + ", " + MAX_MIN_RANGE + "]";
        SEC_BOUND_FORMAT = " of sec " + OUT_OF_ALLOWED_RANGE + MAX_SEC_RANGE + ", " + MAX_SEC_RANGE + "]";
        NSEC_BOUND_FORMAT = " of nsec " + OUT_OF_ALLOWED_RANGE + MAX_NSEC_RANGE + ", " + MAX_NSEC_RANGE + "]";
    }

    public Interval() {
    }

    @Override
    public long get(TemporalUnit unit) {
        if (unit == ChronoUnit.YEARS) {
            return getYear();
        } else if (unit == ChronoUnit.MONTHS) {
            return getMonth();
        } else if (unit == ChronoUnit.WEEKS) {
            return getWeek();
        } else if (unit == ChronoUnit.DAYS) {
            return getDay();
        } else if (unit == ChronoUnit.HOURS) {
            return getHour();
        } else if (unit == ChronoUnit.MINUTES) {
            return getMin();
        } else if (unit == ChronoUnit.SECONDS) {
            return getSec();
        } else if (unit == ChronoUnit.NANOS) {
            return getNsec();
        } else {
            throw new UnsupportedTemporalTypeException("Unsupported unit: " + unit);
        }
    }

    @Override
    public List<TemporalUnit> getUnits() {
        return SUPPORTED_UNITS;
    }

    @Override
    public Temporal addTo(Temporal temporal) {
        temporal = add(temporal, year, YEARS);
        temporal = addMonth(temporal);
        temporal = add(temporal, week, WEEKS);
        temporal = add(temporal, day, DAYS);
        temporal = add(temporal, hour, HOURS);
        temporal = add(temporal, min, MINUTES);
        temporal = add(temporal, sec, SECONDS);
        temporal = add(temporal, nsec, NANOS);
        return temporal;
    }

    private static Temporal add(Temporal temporal, long field, ChronoUnit fieldType) {
        if (field != 0) {
            temporal = temporal.plus(field, fieldType);
        }
        return temporal;
    }

    private Temporal addMonth(Temporal temporal) {
        int oldDay = temporal.get(ChronoField.DAY_OF_MONTH);
        int oldYear = temporal.get(ChronoField.YEAR);
        int oldMonth = temporal.get(ChronoField.MONTH_OF_YEAR);
        if (month != 0) {
            temporal = temporal.plus(month, MONTHS);
        }
        int currentDay = temporal.get(ChronoField.DAY_OF_MONTH);
        if (adjust.equals(Adjust.ExcessAdjust)) {
            return temporal.plus(oldDay - currentDay, DAYS);
        }

        if (adjust.equals(Adjust.LastAdjust)) {
            LocalDate oldYearMonth = LocalDate.of(oldYear, oldMonth, 1);
            int lastDayInOldMonth = oldYearMonth.with(TemporalAdjusters.lastDayOfMonth()).getDayOfMonth();
            if (lastDayInOldMonth == oldDay) {
                return temporal.with(TemporalAdjusters.lastDayOfMonth());
            }
        }
        return temporal;
    }

    @Override
    public Temporal subtractFrom(Temporal temporal) {
        temporal = add(temporal, -year, YEARS);
        temporal = add(temporal, -month, MONTHS);
        temporal = add(temporal, -week, WEEKS);
        temporal = add(temporal, -day, DAYS);
        temporal = add(temporal, -hour, HOURS);
        temporal = add(temporal, -min, MINUTES);
        temporal = add(temporal, -sec, SECONDS);
        temporal = add(temporal, -nsec, NANOS);
        return temporal;
    }

    public long getYear() {
        return year;
    }

    public Interval setYear(long year) {
        Assert.state(-MAX_YEAR_RANGE <= year && year <= MAX_YEAR_RANGE,
                     threadLocalStringBuilder.get()
                                             .delete(0, threadLocalStringBuilder.get().length())
                                             .append(VALUE_BOUND_FORMAT)
                                             .append(year)
                                             .append(YEARS_BOUND_FORMAT)
                                             .toString());
        this.year = year;
        return this;
    }

    public long getMonth() {
        return month;
    }

    public Interval setMonth(long month) {
        Assert.state(-MAX_MONTH_RANGE <= month && month <= MAX_MONTH_RANGE,
                     threadLocalStringBuilder.get()
                                             .delete(0, threadLocalStringBuilder.get().length())
                                             .append(VALUE_BOUND_FORMAT)
                                             .append(month)
                                             .append(MOTH_BOUND_FORMAT)
                                             .toString());
        this.month = month;
        return this;
    }

    public long getWeek() {
        return week;
    }

    public Interval setWeek(long week) {
        Assert.state(-MAX_WEEK_RANGE <= week && week <= MAX_WEEK_RANGE,
                     threadLocalStringBuilder.get()
                                             .delete(0, threadLocalStringBuilder.get().length())
                                             .append(VALUE_BOUND_FORMAT)
                                             .append(week)
                                             .append(WEEK_BOUND_FORMAT)
                                             .toString());
        this.week = week;
        return this;
    }

    public long getDay() {
        return day;
    }

    public Interval setDay(long day) {
        Assert.state(-MAX_DAY_RANGE <= day && day <= MAX_DAY_RANGE,
                     threadLocalStringBuilder.get()
                                             .delete(0, threadLocalStringBuilder.get().length())
                                             .append(VALUE_BOUND_FORMAT)
                                             .append(day)
                                             .append(DAY_BOUND_FORMAT)
                                             .toString());
        this.day = day;
        return this;
    }

    public long getHour() {
        return hour;
    }

    public Interval setHour(long hour) {
        Assert.state(-MAX_HOUR_RANGE <= hour && hour <= MAX_HOUR_RANGE,
                     threadLocalStringBuilder.get()
                                             .delete(0, threadLocalStringBuilder.get().length())
                                             .append(VALUE_BOUND_FORMAT)
                                             .append(hour)
                                             .append(HOUR_BOUND_FORMAT)
                                             .toString());
        this.hour = hour;
        return this;
    }

    public long getMin() {
        return min;
    }

    public Interval setMin(long min) {
        Assert.state(-MAX_MIN_RANGE <= min && min <= MAX_MIN_RANGE,
                     threadLocalStringBuilder.get()
                                             .delete(0, threadLocalStringBuilder.get().length())
                                             .append(VALUE_BOUND_FORMAT)
                                             .append(min)
                                             .append(MIN_BOUND_FORMAT)
                                             .toString());
        this.min = min;
        return this;
    }

    public long getSec() {
        return sec;
    }

    public Interval setSec(long sec) {
        Assert.state(-MAX_SEC_RANGE <= sec && sec <= MAX_SEC_RANGE,
                     threadLocalStringBuilder.get()
                                             .delete(0, threadLocalStringBuilder.get().length())
                                             .append(VALUE_BOUND_FORMAT)
                                             .append(sec)
                                             .append(SEC_BOUND_FORMAT)
                                             .toString());
        this.sec = sec;
        return this;
    }

    public long getNsec() {
        return nsec;
    }

    public Interval setNsec(long nsec) {
        Assert.state(-MAX_NSEC_RANGE <= nsec && nsec <= MAX_NSEC_RANGE,
                     threadLocalStringBuilder.get()
                                             .delete(0, threadLocalStringBuilder.get().length())
                                             .append(VALUE_BOUND_FORMAT)
                                             .append(nsec)
                                             .append(NSEC_BOUND_FORMAT)
                                             .toString());
        this.nsec = nsec;
        return this;
    }

    public Adjust getAdjust() {
        return adjust;
    }

    public Interval setAdjust(Adjust adjust) {
        this.adjust = adjust;
        return this;
    }

    @Override
    public int hashCode() {
        int result = (int) (year ^ (year >>> 32));
        result = 31 * result + (int) (month ^ (month >>> 32));
        result = 31 * result + (int) (week ^ (week >>> 32));
        result = 31 * result + (int) (day ^ (day >>> 32));
        result = 31 * result + (int) (hour ^ (hour >>> 32));
        result = 31 * result + (int) (min ^ (min >>> 32));
        result = 31 * result + (int) (sec ^ (sec >>> 32));
        result = 31 * result + (int) (nsec ^ (nsec >>> 32));
        result = 31 * result + adjust.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Interval interval = (Interval) o;

        if (year != interval.year) {
            return false;
        }
        if (month != interval.month) {
            return false;
        }
        if (week != interval.week) {
            return false;
        }
        if (day != interval.day) {
            return false;
        }
        if (hour != interval.hour) {
            return false;
        }
        if (min != interval.min) {
            return false;
        }
        if (sec != interval.sec) {
            return false;
        }
        if (nsec != interval.nsec) {
            return false;
        }
        return adjust == interval.adjust;
    }

    @Override
    public String toString() {
        StringBuilder sb = threadLocalStringBuilder.get();
        sb.delete(0, sb.length());
        sb.append("Interval{");
        sb.append("year=").append(year);
        sb.append(", month=").append(month);
        sb.append(", week=").append(week);
        sb.append(", day=").append(day);
        sb.append(", hour=").append(hour);
        sb.append(", min=").append(min);
        sb.append(", sec=").append(sec);
        sb.append(", nsec=").append(nsec);
        sb.append(", adjust=").append(adjust);
        sb.append('}');
        return sb.toString();
    }
}
