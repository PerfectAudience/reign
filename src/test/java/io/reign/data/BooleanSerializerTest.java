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
public class BooleanSerializerTest extends AbstractDataSerializerTest {

    private BooleanSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new BooleanSerializer();
    }

    @Override
    @Test
    public void testBasic() throws Exception {
        boolean falseValue = false;
        boolean trueValue = true;

        byte[] bytes;
        Boolean reconstituted;

        // serialize and deserialize multiple times to make sure buffers in serializer are correctly manipulated

        bytes = serializer.serialize(falseValue);

        reconstituted = serializer.deserialize(bytes);
        assertTrue(falseValue + " != " + reconstituted, falseValue == reconstituted);

        reconstituted = serializer.deserialize(bytes);
        assertTrue(falseValue + " != " + reconstituted, falseValue == reconstituted);

        bytes = serializer.serialize(trueValue);
        reconstituted = serializer.deserialize(bytes);
        assertTrue(trueValue + " != " + reconstituted, trueValue == reconstituted);
    }

}
