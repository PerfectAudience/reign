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

package io.reign.examples;

import static io.reign.data.Operators.max;
import static io.reign.data.Operators.min;
import static io.reign.data.Operators.stats;
import static io.reign.data.Operators.sum;
import io.reign.Reign;
import io.reign.data.DataService;
import io.reign.data.MultiData;
import io.reign.data.MultiMapData;
import io.reign.data.Operators.Stats;
import io.reign.data.QueueData;
import io.reign.data.StackData;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class DataServiceExample {
    private static final Logger logger = LoggerFactory.getLogger(DataServiceExample.class);

    public static void main(String[] args) throws Exception {
        /** init and start reign using builder **/
        Reign reign = Reign.maker().zkClient("localhost:2181", 30000).core().get();
        reign.start();

        /** init and start using Spring convenience builder **/
        // SpringReignMaker springReignMaker = new SpringReignMaker();
        // springReignMaker.setZkConnectString("localhost:2181");
        // springReignMaker.setZkSessionTimeout(30000);
        // springReignMaker.setCore(true);
        // springReignMaker.initStart();
        // Reign reign = springReignMaker.get();

        /** data examples **/
        multiDataExample(reign);
        multiMapDataExample(reign);
        queueExample(reign);
        stackExample(reign);

        /** sleep to allow examples to run for a bit **/
        Thread.sleep(600000);

        /** shutdown reign **/
        reign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }

    public static void queueExample(Reign reign) throws Exception {
        DataService dataService = reign.getService("data");
        QueueData<String> queueData = dataService.getQueue("examples", "my-queue-process-safe", Reign.DEFAULT_ACL_LIST);
        queueData.push("value1");
        queueData.push("value2");
        queueData.push("value3");
        queueData.push("value4");
        queueData.push("value5");

        for (int i = 1; i < 6; i++) {
            logger.debug("Expected '{}'; received '{}'", "value" + i, queueData.pop(String.class));
        }
    }

    public static void stackExample(Reign reign) throws Exception {
        DataService dataService = reign.getService("data");
        StackData<String> stackData = dataService.getStack("examples", "my-stack-process-safe", Reign.DEFAULT_ACL_LIST);
        stackData.push("value1");
        stackData.push("value2");
        stackData.push("value3");
        stackData.push("value4");
        stackData.push("value5");

        for (int i = 5; i > 0; i--) {
            logger.debug("Expected '{}'; received '{}'", "value" + i, stackData.pop(String.class));
        }
    }

    public static void multiMapDataExample(Reign reign) throws Exception {
        DataService dataService = reign.getService("data");

        MultiMapData<String> multiMapData;

        // multiMapData = dataService.getMultiMap("examples", "my-multimap", false, Reign.DEFAULT_ACL_LIST);
        // doMultiMapDataExample(multiMapData);

        multiMapData = dataService.getMultiMap("examples", "my-multimap-process-safe", true, Reign.DEFAULT_ACL_LIST);
        doMultiMapDataExample(multiMapData);

    }

    public static void doMultiMapDataExample(MultiMapData<String> multiMapData) throws Exception {
        // logger.debug("stringKey1:  currentValue={}", multiMapData.get("stringKey1", String.class));
        //
        // logger.debug("intKey1Values:  currentValues={}", multiMapData.getAll("intKey1", Integer.class));
        // logger.debug("intKey1Values:  max({})={}", multiMapData.getAll("intKey1", Integer.class), max(multiMapData
        // .getAll("intKey1", Integer.class)));

        multiMapData.put("stringKey1", "stringValue1");
        logger.debug("stringKey1:  value={}", multiMapData.get("stringKey1", String.class));

        multiMapData.put("intKey1", 1);
        multiMapData.put("intKey1", "index1", 2);
        multiMapData.put("intKey1", "index2", 3);
        List<Integer> intKey1Values = multiMapData.getAll("intKey1", Integer.class);
        logger.debug("intKey1Values:  values={}", intKey1Values);
        logger.debug("intKey1Values:  max({})={}", intKey1Values, max(intKey1Values));

        logger.debug("multiMapData.size()={}", multiMapData.size());
        logger.debug("multiMapData.keys()={}", multiMapData.keys());

        // get again, should be from cache this time
        logger.debug("multiMapData.size()={}", multiMapData.size());

        // remove an item from intKey1 and examine values again
        multiMapData.remove("intKey1", "index2");
        intKey1Values = multiMapData.getAll("intKey1", Integer.class);
        logger.debug("After remove():  intKey1Values:  values={}", intKey1Values);
        logger.debug("After remove():  intKey1Values:  max({})={}", intKey1Values, max(intKey1Values));

        // remove default index value from stringKey1
        multiMapData.remove("stringKey1");
        logger.debug("After remove():  stringKey1:  value={}", multiMapData.get("stringKey1", String.class));

        // check sizes after remove operations
        logger.debug("After remove():  multiMapData.size()={}", multiMapData.size());
        logger.debug("After remove():  multiMapData.keys()={}", multiMapData.keys());

        // remove all values
        multiMapData.remove("intKey1");
        multiMapData.removeAll("stringKey1");

        // check sizes after remove operations
        logger.debug("After remove():  multiMapData.size()={}", multiMapData.size());
        logger.debug("After remove():  multiMapData.keys()={}", multiMapData.keys());

        // check values after removeAll operations
        intKey1Values = multiMapData.getAll("intKey1", Integer.class);
        logger.debug("After removeAll(): intKey1Values:  values={}", intKey1Values);
        logger.debug("After removeAll(): intKey1Values:  max({})={}", intKey1Values, max(intKey1Values));
        logger.debug("After removeAll(): stringKey1:  value={}", multiMapData.get("stringKey1", String.class));

        // check sizes after removeAll operations
        logger.debug("After removeAll():  multiMapData.size()={}", multiMapData.size());
        logger.debug("After removeAll():  multiMapData.keys()={}", multiMapData.keys());

    }

    public static void multiDataExample(Reign reign) throws Exception {
        DataService dataService = reign.getService("data");

        MultiData<Double> multiData;

        multiData = dataService.getMulti("examples", "my-data", false, Reign.DEFAULT_ACL_LIST);
        doMultiDataExample(multiData);

        multiData = dataService.getMulti("examples", "my-data-process-safe", true, Reign.DEFAULT_ACL_LIST);
        doMultiDataExample(multiData);

    }

    public static void doMultiDataExample(MultiData<Double> multiData) throws Exception {
        multiData.set("1", 1000D);
        multiData.set("2", 2000D);

        List<Double> values = multiData.getAll(Double.class);
        logger.debug("MultiData:  values={}", values);

        Double value1 = multiData.get("1", Double.class);
        logger.debug("MultiData:  value1={}", value1);

        multiData.set(19999.50);
        multiData.set("1", 10000D);

        values = multiData.getAll(Double.class);
        logger.debug("MultiData:  values={}", values);

        double sum = sum(values);
        logger.debug("MultiData:  sum({})={}", values, sum);

        double max = max(values);
        logger.debug("MultiData:  max({})={}", values, max);

        double min = min(values);
        logger.debug("MultiData:  min({})={}", values, min);

        Stats<Double> stats = stats(values);
        logger.debug("MultiData:  stats({}):  avg={}; stdDev={}; min={}; max={}; sum={}",
                new Object[] { values, stats.avg(), stats.stdDev(), stats.min(), stats.max(), stats.sum() });

        // sleep 5 seconds to allow data to "age"
        Thread.sleep(5000);

        // get data less than 10 seconds old, should return same results as before
        values = multiData.getAll(10000, Double.class);
        stats = stats(values);
        logger.debug("MultiData (10000 tll):  stats({}):  avg={}; stdDev={}; min={}; max={}; sum={}", new Object[] {
                values, stats.avg(), stats.stdDev(), stats.min(), stats.max(), stats.sum() });

        // get data less than 4 seconds old, should return nothing
        values = multiData.getAll(4000, Double.class);
        stats = stats(values);
        logger.debug("MultiData (4000 ttl):  stats({}):  avg={}; stdDev={}; min={}; max={}; sum={}", new Object[] {
                values, stats.avg(), stats.stdDev(), stats.min(), stats.max(), stats.sum() });

        multiData.remove();
        values = multiData.getAll(Double.class);
        logger.debug("MultiData:  values={}", values);

        multiData.removeAll();
        values = multiData.getAll(Double.class);
        logger.debug("MultiData:  values={}", values);
    }
}
