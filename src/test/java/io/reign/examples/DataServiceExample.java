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
import io.reign.data.Operators.Stats;

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
        // springReignMaker.initializeAndStart();
        // Reign reign = springReignMaker.get();

        /** messaging example **/
        multiDataExample(reign);

        /** sleep to allow examples to run for a bit **/
        Thread.sleep(600000);

        /** shutdown reign **/
        reign.stop();

        /** sleep a bit to observe observer callbacks **/
        Thread.sleep(10000);
    }

    public static void multiDataExample(Reign reign) throws Exception {
        DataService dataService = reign.getService("data");

        MultiData<Double> multiData = dataService.getMulti("examples", "my-data", Double.class, false,
                Reign.DEFAULT_ACL_LIST);
        doMultiDataExample(multiData);

        multiData = dataService
                .getMulti("examples", "my-data-process-safe", Double.class, true, Reign.DEFAULT_ACL_LIST);
        doMultiDataExample(multiData);

    }

    public static void doMultiDataExample(MultiData<Double> multiData) throws Exception {
        multiData.set("1", 1000D);
        multiData.set("2", 2000D);

        List<Double> values = multiData.getAll();
        logger.debug("MultiData:  values={}", values);

        Double value1 = multiData.get("1");
        logger.debug("MultiData:  value1={}", value1);

        multiData.set(19999.50);
        multiData.set("1", 10000D);

        values = multiData.getAll();
        logger.debug("MultiData:  values={}", values);

        double sum = sum(values);
        logger.debug("MultiData:  sum({})={}", values, sum);

        double max = max(values);
        logger.debug("MultiData:  max({})={}", values, max);

        double min = min(values);
        logger.debug("MultiData:  min({})={}", values, min);

        Stats<Double> stats = stats(values);
        logger.debug("MultiData:  stats({}):  avg={}; stdDev={}; min={}; max={}; sum={}", new Object[] { values,
                stats.avg(), stats.stdDev(), stats.min(), stats.max(), stats.sum() });

        // sleep 5 seconds to allow data to "age"
        Thread.sleep(5000);

        // get data less than 10 seconds old, should return same results as before
        values = multiData.getAll(10000);
        stats = stats(values);
        logger.debug("MultiData (10000 tll):  stats({}):  avg={}; stdDev={}; min={}; max={}; sum={}", new Object[] {
                values, stats.avg(), stats.stdDev(), stats.min(), stats.max(), stats.sum() });

        // get data less than 4 seconds old, should return nothing
        values = multiData.getAll(4000);
        stats = stats(values);
        logger.debug("MultiData (4000 ttl):  stats({}):  avg={}; stdDev={}; min={}; max={}; sum={}", new Object[] {
                values, stats.avg(), stats.stdDev(), stats.min(), stats.max(), stats.sum() });

        multiData.remove();
        values = multiData.getAll();
        logger.debug("MultiData:  values={}", values);

        multiData.removeAll();
        values = multiData.getAll();
        logger.debug("MultiData:  values={}", values);
    }
}
