package io.reign;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reign.mesg.RequestMessage;
import io.reign.mesg.ResponseMessage;
import io.reign.mesg.SimpleResponseMessage;
import io.reign.presence.PresenceService;

/**
 * No-op service.
 * 
 * @author ypai
 * 
 */
public class NullService extends AbstractService {

    private static final Logger logger = LoggerFactory.getLogger(NullService.class);

    @Override
    public void init() {

    }

    @Override
    public void destroy() {

    }

    @Override
    public ResponseMessage handleMessage(RequestMessage requestMessage) {
        if (logger.isTraceEnabled()) {
            logger.trace("Received message:  request='{}:{}'", requestMessage.getTargetService(),
                    requestMessage.getBody());
        }
        return SimpleResponseMessage.DEFAULT_OK_RESPONSE;
    }

}
