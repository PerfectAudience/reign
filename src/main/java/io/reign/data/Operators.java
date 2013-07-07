package io.reign.data;

/**
 * 
 * @author ypai
 * 
 */
public class Operators {

    public static <T extends Number> T sum(Iterable<T> values) {
        Number result = 0;
        for (T value : values) {
            if (value instanceof Long) {
                result = result.longValue() + value.longValue();
            } else if (value instanceof Double) {
                result = result.doubleValue() + value.doubleValue();
            } else if (value instanceof Float) {
                result = result.floatValue() + value.floatValue();
            } else if (value instanceof Integer) {
                result = result.intValue() + value.intValue();
            } else if (value instanceof Short) {
                result = result.shortValue() + value.shortValue();
            } else if (value instanceof Byte) {
                result = result.byteValue() + value.byteValue();
            }
        }
        return (T) result;
    }

    public static <T extends Comparable> T min(Iterable<T> values) {
        Comparable result = null;
        for (Comparable value : values) {
            if (result == null) {
                result = value;
            } else {
                if (value.compareTo(result) < 0) {
                    result = value;
                }
            }
        }
        return (T) result;
    }

    public static <T extends Comparable> T max(Iterable<T> values) {
        Comparable result = null;
        for (Comparable value : values) {
            if (result == null) {
                result = value;
            } else {
                if (value.compareTo(result) > 0) {
                    result = value;
                }
            }
        }
        return (T) result;
    }

    public static <T extends Number> Stats stats(Iterable<T> values) {
        int count = 0;
        double avg = 0.0;
        double pwrSumAvg = 0.0;
        double stdDev = 0.0;

        for (Number value : values) {
            double valueAsDouble = value.doubleValue();
            count++;
            avg += (valueAsDouble - avg) / count;
            pwrSumAvg += (valueAsDouble * valueAsDouble - pwrSumAvg) / count;
        }

        // use this since we have the complete data set
        stdDev = Math.sqrt((pwrSumAvg * count - count * avg * avg) / (count));

        // if we were doing a running sample, we would use this formula:
        // stdDev = Math.sqrt((pwrSumAvg * count - count * avg * avg) / (count-1));

        return new Stats(avg, stdDev);
    }

    public static Boolean and(Iterable<Boolean> values) {
        for (Boolean value : values) {
            if (!value) {
                return false;
            }
        }
        return true;
    }

    public static Boolean or(Iterable<Boolean> values) {
        for (Boolean value : values) {
            if (value) {
                return true;
            }
        }
        return false;
    }

    /**
     * Result returned when calculating basic stats.
     * 
     * @author ypai
     * 
     */
    public static class Stats {
        private final double avg;
        private final double stdDev;

        public Stats(double avg, double stdDev) {
            this.avg = avg;
            this.stdDev = stdDev;
        }

        public double avg() {
            return avg;
        }

        public double stdDev() {
            return stdDev;
        }
    }

}
