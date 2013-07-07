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

        Stats stats = stats(values);
        logger.debug("MultiData:  stats({}):  avg={}; stdDev={}", new Object[] { values, stats.avg(), stats.stdDev() });

        multiData.remove();
        values = multiData.getAll();
        logger.debug("MultiData:  values={}", values);

        multiData.removeAll();
        values = multiData.getAll();
        logger.debug("MultiData:  values={}", values);
    }
}
