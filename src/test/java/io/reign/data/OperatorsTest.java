package io.reign.data;

import static org.junit.Assert.assertTrue;
import io.reign.data.Operators.Stats;
import io.reign.util.Structs;

import org.junit.Test;

/**
 * 
 * @author ypai
 * 
 */
public class OperatorsTest {

    @Test
    public void testSum() throws Exception {
        long result;

        result = Operators.sum(Structs.<Long> list().v(1L).v(2L).v(3L));
        assertTrue("sum = " + result + " ?", result == 6);

        result = Operators.sum(Structs.<Long> iterable(new Long[] { 1L, 2L, 3L }));
        assertTrue("sum = " + result + " ?", result == 6);
    }

    @Test
    public void testMin() throws Exception {
        long result = Operators.min(Structs.<Long> iterable(new Long[] { 1L, 2L, 3L }));
        assertTrue("min = " + result + " ?", result == 1);
    }

    @Test
    public void testMax() throws Exception {
        long result = Operators.max(Structs.<Long> iterable(new Long[] { 1L, 2L, 3L }));
        assertTrue("max = " + result + " ?", result == 3L);
    }

    @Test
    public void testAnd() throws Exception {
        boolean result;

        result = Operators.and(Structs.<Boolean> iterable(new Boolean[] { true, false, false, true, false, true }));
        assertTrue("and = " + result + " ?", !result);

        result = Operators.and(Structs.<Boolean> iterable(new Boolean[] { true, true, true, true, true }));
        assertTrue("and = " + result + " ?", result);
    }

    @Test
    public void testOr() throws Exception {
        boolean result;

        result = Operators.or(Structs.<Boolean> iterable(new Boolean[] { true, false, false, true, false, true }));
        assertTrue("and = " + result + " ?", result);

        result = Operators.or(Structs.<Boolean> iterable(new Boolean[] { false, false, false, false, false }));
        assertTrue("and = " + result + " ?", !result);
    }

    @Test
    public void testStats() throws Exception {
        Stats<Integer> stats;

        stats = Operators.stats(Structs.<Integer> iterable(new Integer[] { 2, 4, 4, 4, 5, 5, 7, 9 }));
        assertTrue("avg = " + stats.avg() + " ?", stats.avg() == 5.0);
        assertTrue("stdDev = " + stats.stdDev() + " ?", stats.stdDev() == 2.0);
        assertTrue("min = " + stats.min() + " ?", stats.min() == 2);
        assertTrue("max = " + stats.max() + " ?", stats.max() == 9);
        assertTrue("sum = " + stats.sum() + " ?", stats.sum() == 40);

    }
}
