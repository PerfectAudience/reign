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
public class ByteSerializerTest extends AbstractDataSerializerTest {

    private ByteSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new ByteSerializer();
    }

    @Override
    @Test
    public void testBasic() throws Exception {
        byte data = 5;

        byte[] bytes = serializer.serialize(data);

        Byte reconstituted = serializer.deserialize(bytes);

        assertTrue(data + " != " + reconstituted, data == reconstituted);
    }
}
