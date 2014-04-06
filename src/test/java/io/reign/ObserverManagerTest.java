/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package io.reign;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class ObserverManagerTest {

    @Test
    public void testChildListsDiffer() throws Exception {
        List<String> list1 = new ArrayList<String>();
        list1.add("A");
        list1.add("B");
        list1.add("C");
        List<String> list2 = new ArrayList<String>();
        list2.add("A");
        list2.add("B");
        list2.add("C");
        List<String> list3 = new ArrayList<String>();
        list3.add("A");
        list3.add("B");
        list3.add("C");
        list3.add("D");
        List<String> list4 = new ArrayList<String>();
        list4.add("B");
        list4.add("C");
        list4.add("D");

        assertFalse(ObserverManager.childListsDiffer(Collections.EMPTY_LIST, Collections.EMPTY_LIST));
        assertFalse(ObserverManager.childListsDiffer(null, null));
        assertFalse(ObserverManager.childListsDiffer(list1, list2));
        assertTrue(ObserverManager.childListsDiffer(list1, list3));
        assertTrue(ObserverManager.childListsDiffer(list1, list4));
    }
}
