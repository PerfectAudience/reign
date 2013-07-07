package io.reign;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

/**
 * 
 * @author ypai
 * 
 */
public abstract class AbstractDataSerializerTest {

    @Test
    public abstract void testBasic() throws Exception;

    @Test
    public void testMultiThreaded() throws Exception {
        final AtomicReference<Throwable> throwableRef = new AtomicReference<Throwable>();

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 1000; i++) {
                        try {
                            testBasic();
                        } catch (AssertionError e) {
                            throwableRef.set(e);
                            break;
                        } catch (Exception e) {
                            throwableRef.set(e);
                            break;
                        }
                    }
                }
            });
        }
        executorService.shutdown();

        // if we don't sleep, error references may not be immediately visible
        Thread.sleep(100);

        assertTrue("error=" + throwableRef.get(), throwableRef.get() == null);

    }
}
