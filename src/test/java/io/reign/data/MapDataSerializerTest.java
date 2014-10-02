package io.reign.data;

import io.reign.AbstractDataSerializerTest;
import io.reign.DataSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;

/**
 * Service discovery service.
 * 
 * @author francoislagier
 * 
 */
public class MapDataSerializerTest extends AbstractDataSerializerTest {

    private DataSerializer<Map<String, String>> serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new MapDataSerializer<Map<String, String>>();
    }

    @Override
    public void testBasic() throws Exception {
        Map<String, String> originalMap = new HashMap<String, String>();
        for (int j = 0; j < 100; j++) {
            originalMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }

        byte[] bytes = serializer.serialize(originalMap);

        Map<String, String> reconstructedMap = serializer.deserialize(bytes);

        Assert.assertTrue(reconstructedMap.size() == originalMap.size());
        for (Entry<String, String> entry : originalMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            Assert.assertTrue(reconstructedMap.containsKey(key));
            Assert.assertTrue(reconstructedMap.get(key).equals(value));
        }
    }

}
