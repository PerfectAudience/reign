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

    public static <T extends Number> Stats<T> stats(Iterable<T> values) {
        int count = 0;
        double avg = 0;
        double pwrSumAvg = 0.0;
        double stdDev = 0;
        Number min = null;
        Number max = null;
        Number sum = 0;

        for (T value : values) {
            // avg and std dev.
            double valueAsDouble = value.doubleValue();
            count++;
            avg += (valueAsDouble - avg) / count;
            pwrSumAvg += (valueAsDouble * valueAsDouble - pwrSumAvg) / count;

            // min, max, sum
            if (max == null) {
                max = value;
            }
            if (min == null) {
                min = value;
            }
            if (value instanceof Long) {
                if (value.longValue() > max.longValue()) {
                    max = value;
                }
                if (value.longValue() < min.longValue()) {
                    min = value;
                }
                sum = sum.longValue() + value.longValue();
            } else if (value instanceof Double) {
                if (value.doubleValue() > max.doubleValue()) {
                    max = value;
                }
                if (value.doubleValue() < min.doubleValue()) {
                    min = value;
                }
                sum = sum.doubleValue() + value.doubleValue();
            } else if (value instanceof Float) {
                if (value.floatValue() > max.floatValue()) {
                    max = value;
                }
                if (value.floatValue() < min.floatValue()) {
                    min = value;
                }
                sum = sum.floatValue() + value.floatValue();
            } else if (value instanceof Integer) {
                if (value.intValue() > max.intValue()) {
                    max = value;
                }
                if (value.intValue() < min.intValue()) {
                    min = value;
                }
                sum = sum.intValue() + value.intValue();
            } else if (value instanceof Short) {
                if (value.shortValue() > max.shortValue()) {
                    max = value;
                }
                if (value.shortValue() < min.shortValue()) {
                    min = value;
                }
                sum = sum.shortValue() + value.shortValue();
            } else if (value instanceof Byte) {
                if (value.byteValue() > max.byteValue()) {
                    max = value;
                }
                if (value.byteValue() < min.byteValue()) {
                    min = value;
                }
                sum = sum.byteValue() + value.byteValue();
            }
        }

        // use this since we have the complete data set
        if (count > 0) {
            stdDev = Math.sqrt((pwrSumAvg * count - count * avg * avg) / (count));
        }

        // if there were no items in set
        if (max == null) {
            max = (byte) 0;
        }
        if (min == null) {
            min = (byte) 0;
        }

        // if we were doing a running sample, we would use this formula:
        // stdDev = Math.sqrt((pwrSumAvg * count - count * avg * avg) / (count-1));

        return new Stats<T>(avg, stdDev, (T) min, (T) max, (T) sum);
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
    public static class Stats<T extends Number> {
        private final double avg;
        private final double stdDev;
        private final T min;
        private final T max;
        private final T sum;

        public Stats(double avg, double stdDev, T min, T max, T sum) {
            this.avg = avg;
            this.stdDev = stdDev;
            this.min = min;
            this.max = max;
            this.sum = sum;
        }

        public double avg() {
            return avg;
        }

        public double stdDev() {
            return stdDev;
        }

        public T min() {
            return min;
        }

        public T max() {
            return max;
        }

        public T sum() {
            return sum;
        }
    }

}
