package io.reign.data;

import static org.junit.Assert.assertTrue;
import io.reign.AbstractDataSerializerTest;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author ypai
 * 
 */
public class BytesSerializerTest extends AbstractDataSerializerTest {

    private BytesSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new BytesSerializer();
    }

    @Override
    @Test
    public void testBasic() throws Exception {
        byte[] data = new byte[] { 1, 2, 3, 4 };

        byte[] bytes = serializer.serialize(data);

        assertTrue(Arrays.equals(data, bytes));

        byte[] reconstituted = serializer.deserialize(bytes);

        assertTrue(Arrays.equals(data, reconstituted));
    }
}
