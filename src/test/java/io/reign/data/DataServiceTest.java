package io.reign.data;

import static org.junit.Assert.*;
import io.reign.MasterTestSuite;
import io.reign.Reign;

import org.junit.Before;
import org.junit.Test;

public class DataServiceTest {

    private DataService dataService;

    @Before
    public void setUp() throws Exception {
        dataService = MasterTestSuite.getReign().getService("data");
    }

    @Test
    public void testGetMultiStringString() {
        // fail("Not yet implemented");
    }

    @Test
    public void testGetMultiMapStringString() {
        // fail("Not yet implemented");
    }

    @Test
    public void testGetLinkedListStringString() {
        LinkedListData<String> linkedListData = dataService.getLinkedList("examples", "my-linkedlist-process-safe",
                Reign.DEFAULT_ACL_LIST);
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
        QueueData<String> queueData = dataService.getQueue("examples", "my-queue-process-safe", Reign.DEFAULT_ACL_LIST);
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
        StackData<String> stackData = dataService.getStack("examples", "my-stack-process-safe", Reign.DEFAULT_ACL_LIST);
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
