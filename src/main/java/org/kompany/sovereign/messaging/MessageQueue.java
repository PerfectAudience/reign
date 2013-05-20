package org.kompany.sovereign.messaging;

import java.util.concurrent.TimeUnit;

/**
 * 
 * @author ypai
 * 
 */
public interface MessageQueue {

    public boolean offer(Message message);

    public Message poll();

    public Message poll(long time, TimeUnit unit) throws InterruptedException;
}
