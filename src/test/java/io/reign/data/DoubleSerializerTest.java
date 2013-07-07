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
public class DoubleSerializerTest extends AbstractDataSerializerTest {

    private DoubleSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new DoubleSerializer();
    }

    @Override
    @Test
    public void testBasic() throws Exception {

        double data1 = Double.MAX_VALUE;
        double data2 = Double.MIN_VALUE;

        byte[] bytes1;
        byte[] bytes2;
        Double reconstituted;

        // serialize and deserialize multiple times to make sure buffers in serializer are correctly manipulated

        bytes1 = serializer.serialize(data1);
        bytes2 = serializer.serialize(data2);

        reconstituted = serializer.deserialize(bytes1);
        assertTrue(data1 + " != " + reconstituted, data1 == reconstituted);

        reconstituted = serializer.deserialize(bytes1);
        assertTrue(data1 + " != " + reconstituted, data1 == reconstituted);

        bytes2 = serializer.serialize(data2);
        reconstituted = serializer.deserialize(bytes2);
        assertTrue(data2 + " != " + reconstituted, data2 == reconstituted);
    }
}
