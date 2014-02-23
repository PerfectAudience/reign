package io.reign;

import io.reign.mesg.RequestMessage;
import io.reign.mesg.ResponseMessage;
import io.reign.mesg.ResponseStatus;
import io.reign.mesg.SimpleResponseMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        // return SimpleResponseMessage.DEFAULT_OK_RESPONSE;
        ResponseMessage responseMessage = new SimpleResponseMessage(ResponseStatus.OK);
        responseMessage.setId(requestMessage.getId());
        return responseMessage;
    }

}
