package io.reign.mesg;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParsedRequestMessageTest {
    private static final Logger logger = LoggerFactory.getLogger(ParsedRequestMessageTest.class);

    @Test
    public void testBasic() throws Exception {
        ParsedRequestMessage parsed = null;

        parsed = parsedRequestMessage("/cluster1/serviceA");
        assertTrue(parsed.getMeta() == null);
        assertTrue("/cluster1/serviceA".equals(parsed.getResource()));

        parsed = parsedRequestMessage("/cluster1/serviceA    ");
        assertTrue(parsed.getMeta() == null);
        assertTrue("/cluster1/serviceA".equals(parsed.getResource()));

        parsed = parsedRequestMessage("    /cluster1/serviceA    ");
        assertTrue(parsed.getMeta() == null);
        assertTrue("/cluster1/serviceA".equals(parsed.getResource()));

        parsed = parsedRequestMessage("/cluster1/serviceA#meta\nkey1=val1");
        assertTrue("Unexpected value:  " + parsed.getMeta(), "meta".equals(parsed.getMeta()));
        assertTrue("/cluster1/serviceA".equals(parsed.getResource()));

        parsed = parsedRequestMessage("/cluster1/serviceA #meta\n  key1=val1\nkey2=val2\n");
        assertTrue("meta".equals(parsed.getMeta()));
        assertTrue("Unexpected value:  " + parsed.getResource(), "/cluster1/serviceA".equals(parsed.getResource()));

        parsed = parsedRequestMessage("   /cluster1/serviceA #meta\n  key1=val1\nkey2=val2\n");
        assertTrue("meta".equals(parsed.getMeta()));
        assertTrue("/cluster1/serviceA".equals(parsed.getResource()));

        parsed = parsedRequestMessage("/cluster1/serviceA #meta  ");
        assertTrue("meta".equals(parsed.getMeta()));
        assertTrue("/cluster1/serviceA".equals(parsed.getResource()));

        parsed = parsedRequestMessage("/cluster1/serviceA #meta  \n");
        assertTrue("meta".equals(parsed.getMeta()));
        assertTrue("/cluster1/serviceA".equals(parsed.getResource()));

        parsed = parsedRequestMessage("   /cluster1/serviceA #meta  \n");
        assertTrue("meta".equals(parsed.getMeta()));
        assertTrue("/cluster1/serviceA".equals(parsed.getResource()));

    }

    ParsedRequestMessage parsedRequestMessage(String message) {
        SimpleRequestMessage requestMessage = new SimpleRequestMessage("test", message);
        return new ParsedRequestMessage(requestMessage);
    }
}
