package io.reign;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author ypai
 * 
 */
public class DefaultPathSchemeTest {
    private DefaultPathScheme pathScheme;

    @Before
    public void setUp() throws Exception {
        pathScheme = new DefaultPathScheme();
    }

    @Test
    public void testGetParentPath() throws Exception {
        String value;
        value = pathScheme.getParentPath("/a/b/c");
        assertTrue("/a/b".equals(value));

        try {
            value = pathScheme.getParentPath("/a/b/c/");
            assertTrue("Should not get here", false);
        } catch (Exception e) {
        }

        value = pathScheme.getParentPath("a/b/c");
        assertTrue("a/b".equals(value));

        try {
            value = pathScheme.getParentPath("a/b/c/");
            assertTrue("Should not get here", false);
        } catch (Exception e) {
        }

    }

    @Test
    public void testJoin() throws Exception {

        String value;

        value = pathScheme.joinPaths("/this/that", "and/how");
        assertTrue("Unexpected value:  " + value, "/this/that/and/how".equals(value));

        value = pathScheme.joinPaths("/this/that", "/and/how");
        assertTrue("Unexpected value:  " + value, "/this/that/and/how".equals(value));

        try {
            value = pathScheme.joinPaths("/this/that", "/and/how/");
            assertTrue("Should not get here", false);
        } catch (Exception e) {
        }

        try {
            value = pathScheme.joinPaths("/this/that", "and/how/");
            assertTrue("Should not get here", false);
        } catch (Exception e) {
        }
    }

    @Test
    public void testToPathToken() throws Exception {
        String value;
        value = pathScheme.toPathToken(new DefaultCanonicalId("1234", "1.2.3.4", "localhost", 4321, 9876));

        // System.out.println(value);

        CanonicalId id = pathScheme.parseCanonicalId(value);

        assertTrue("1234".equals(id.getProcessId()));
        assertTrue("1.2.3.4".equals(id.getIpAddress()));
        assertTrue("localhost".equals(id.getHost()));
        assertTrue(4321 == id.getPort());
        assertTrue(9876 == id.getMessagingPort());
    }
}
