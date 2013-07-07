package io.reign.data;

import static org.junit.Assert.assertTrue;
import io.reign.AbstractDataSerializerTest;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author ypai
 * 
 */
public class Utf8StringSerializerTest extends AbstractDataSerializerTest {

    private Utf8StringSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new Utf8StringSerializer();
    }

    @Override
    @Test
    public void testBasic() throws Exception {
        String data = "Superduper man with some funky chars:  Û";

        byte[] bytes = serializer.serialize(data);

        String reconstituted = serializer.deserialize(bytes);

        assertTrue(data + " != " + reconstituted, data.equals(reconstituted));
    }
}
