package io.reign.coord;

import static org.junit.Assert.assertTrue;
import io.reign.DefaultPathScheme;
import io.reign.PathScheme;
import io.reign.coord.CoordObserver;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class CoordObserverTest {

    @Test
    public void testFindRevoked() {
        List<String> previous = new ArrayList<String>();
        previous.add("b");
        previous.add("c");
        previous.add("d");
        previous.add("e");

        List<String> updated = new ArrayList<String>();
        updated.add("a");
        updated.add("b");
        updated.add("d");
        updated.add("e");

        PathScheme pathScheme = new DefaultPathScheme("/reign", "reign");
        List<String> revoked = CoordObserver.findRevoked(updated, previous, "/entity/path", pathScheme);

        assertTrue("Unexpected value:  " + revoked, revoked.size() == 1 && revoked.contains("/entity/path/c"));

        // b c d e
        // e f g h
    }
}
