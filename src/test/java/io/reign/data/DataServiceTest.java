package io.reign.data;

import static io.reign.data.Operators.max;
import static org.junit.Assert.*;

import java.util.List;

import io.reign.MasterTestSuite;
import io.reign.Reign;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Smoke test for basic data functionality.
 * 
 * @author ypai
 * 
 */
public class DataServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(DataServiceTest.class);

    private DataService dataService;

    @Before
    public void setUp() throws Exception {
        dataService = MasterTestSuite.getReign().getService("data");
    }

    @Test
    public void testGetMultiStringString() {
        MultiData<Integer> multiData = dataService.getMulti("examples", "my-data-process-safe", true,
                Reign.DEFAULT_ACL_LIST);

        multiData.set("value1", 1000);
        multiData.set("value2", 2000);

        List<Integer> values = multiData.getAll(Integer.class);
        logger.debug("MultiData:  values=" + values, values);

        Integer value1 = multiData.get("value1", Integer.class);
        assertTrue("MultiData:  expected 1000; got " + value1, value1 == 1000);

        multiData.set(19999);
        Integer defaultValue = multiData.get(Integer.class);
        assertTrue("MultiData:  expected 19999; got " + defaultValue, defaultValue == 19999);

        value1 = multiData.get("value1", Integer.class);
        assertTrue("MultiData:  expected 1000; got " + value1, value1 == 1000);

        multiData.set("value1", 10000);
        value1 = multiData.get("value1", Integer.class);
        assertTrue("MultiData:  expected 10000; got " + value1, value1 == 10000);

        values = multiData.getAll(Integer.class);
        logger.debug("MultiData:  values={}", values);
    }

    @Test
    public void testGetMultiMapStringString() {
        MultiMapData<String> multiMapData = dataService.getMultiMap("examples", "my-multimap-process-safe");

        multiMapData.put("stringKey1", "stringValue1");
        logger.debug("stringKey1:  value={}", multiMapData.get("stringKey1", String.class));

        multiMapData.put("intKey1", 1);
        multiMapData.put("intKey1", "index1", 2);
        multiMapData.put("intKey1", "index2", 3);
        List<Integer> intKey1Values = multiMapData.getAll("intKey1", Integer.class);
        logger.debug("intKey1Values:  values={}", intKey1Values);
        logger.debug("intKey1Values:  max({})={}", intKey1Values, max(intKey1Values));
        assertTrue(max(intKey1Values) == 3);

        logger.debug("multiMapData.size()={}", multiMapData.size());
        logger.debug("multiMapData.keys()={}", multiMapData.keys());
        assertTrue(multiMapData.size() == 2);
        assertTrue(multiMapData.keys().size() == 2);

        // get again, should be from cache this time
        logger.debug("multiMapData.size()={}", multiMapData.size());
        assertTrue(multiMapData.size() == 2);

        // remove an item from intKey1 and examine values again
        multiMapData.remove("intKey1", "index2");
        intKey1Values = multiMapData.getAll("intKey1", Integer.class);
        logger.debug("After remove():  intKey1Values:  values={}", intKey1Values);
        logger.debug("After remove():  intKey1Values:  max({})={}", intKey1Values, max(intKey1Values));
        assertTrue(max(intKey1Values) == 2);

        // remove default index value from stringKey1
        multiMapData.remove("stringKey1");
        logger.debug("After remove():  stringKey1:  value={}", multiMapData.get("stringKey1", String.class));

        // check sizes after remove operations
        logger.debug("After remove():  multiMapData.size()={}", multiMapData.size());
        logger.debug("After remove():  multiMapData.keys()={}", multiMapData.keys());

        // remove 1 of 2 remaining (including default) values from this key
        multiMapData.remove("intKey1");

        // remove all values from this key
        multiMapData.removeAll("stringKey1");
        logger.debug("After removeAll(): stringKey1:  value={}", multiMapData.get("stringKey1", String.class));

        assertTrue("Got " + multiMapData.size(), multiMapData.size() == 1);
        assertTrue(multiMapData.keys().size() == 1);
        assertTrue(multiMapData.keys().get(0).equals("intKey1"));

        // check values after removeAll operations
        intKey1Values = multiMapData.getAll("intKey1", Integer.class);
        logger.debug("After removeAll(): intKey1Values:  values={}", intKey1Values);
        logger.debug("After removeAll(): intKey1Values:  max({})={}", intKey1Values, max(intKey1Values));
        assertTrue(intKey1Values.size() == 1);
        assertTrue(intKey1Values.get(0).equals(2));

        // remove rest of data for "intKey1"
        multiMapData.remove("intKey1", "index1");

        // check sizes after removing all values
        assertTrue("Expected 0; got " + multiMapData.keys().size() + "; keys=" + multiMapData.keys()
                + "; intKey1Values=" + intKey1Values, multiMapData.keys().size() == 0);
        assertTrue("Expected 0; got " + multiMapData.size(), multiMapData.size() == 0);

    }

    @Test
    public void testGetLinkedListStringString() {
        LinkedListData<String> linkedListData = dataService.getLinkedList("examples", "my-linkedlist-process-safe");
        while (linkedListData.size() > 0) {
            linkedListData.popFirst(String.class);
        }
        linkedListData.pushLast("value1");
        linkedListData.pushLast("value2");
        linkedListData.pushLast("value3");
        linkedListData.pushLast("value4");
        linkedListData.pushLast("value5");

        String value = linkedListData.peekAt(2, String.class);
        assertTrue("Expected 'value3'; received " + value, "value3".equals(value));

        value = linkedListData.popAt(2, String.class);
        assertTrue("Expected 'value3'; received " + value, "value3".equals(value));

        String[] valueArray = new String[] { "value1", "value2", "value4", "value5" };
        for (int i = 1; i < 5; i++) {
            value = linkedListData.popFirst(String.class);
            assertTrue("Expected '" + valueArray[i - 1] + "'; received '" + value + "'",
                    (valueArray[i - 1]).equals(value));
        }
    }

    @Test
    public void testGetQueueStringString() {
        QueueData<String> queueData = dataService.getQueue("examples", "my-queue-process-safe");
        while (queueData.size() > 0) {
            queueData.pop(String.class);
        }
        queueData.push("value1");
        queueData.push("value2");
        queueData.push("value3");
        queueData.push("value4");
        queueData.push("value5");

        for (int i = 1; i < 6; i++) {
            String value = queueData.pop(String.class);
            assertTrue("Expected 'value" + i + "'; received '" + value + "'", ("value" + i).equals(value));
        }
    }

    @Test
    public void testGetStackStringString() {
        StackData<String> stackData = dataService.getStack("examples", "my-stack-process-safe");
        while (stackData.size() > 0) {
            stackData.pop(String.class);
        }
        stackData.push("value1");
        stackData.push("value2");
        stackData.push("value3");
        stackData.push("value4");
        stackData.push("value5");

        for (int i = 5; i > 0; i--) {
            String value = stackData.pop(String.class);
            assertTrue("Expected 'value" + i + "'; received '" + value + "'", ("value" + i).equals(value));
        }
    }
}
