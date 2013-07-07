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
public class IntegerSerializerTest extends AbstractDataSerializerTest {

    private IntegerSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new IntegerSerializer();
    }

    @Override
    @Test
    public void testBasic() throws Exception {
        int data1 = Integer.MAX_VALUE;
        int data2 = Integer.MIN_VALUE;

        byte[] bytes1;
        byte[] bytes2;
        Integer reconstituted;

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
