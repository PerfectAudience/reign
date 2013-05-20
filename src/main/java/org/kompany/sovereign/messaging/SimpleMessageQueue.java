package org.kompany.sovereign.messaging;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author ypai
 * 
 */
public class SimpleMessageQueue implements MessageQueue {

    private final LinkedBlockingDeque<Message> queue = new LinkedBlockingDeque<Message>(50000);

    @Override
    public boolean offer(Message message) {
        return queue.offer(message);

    }

    @Override
    public Message poll() {
        return queue.poll();
    }

    @Override
    public Message poll(long time, TimeUnit unit) throws InterruptedException {
        return queue.poll(time, unit);
    }

}
