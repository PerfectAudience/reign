package org.kompany.sovereign.messaging;

import java.util.regex.Pattern;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class DefaultMessageProtocol implements MessageProtocol {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageProtocol.class);

    private static final Pattern PATTERN_TEXT_REQUEST_SPLITTER = Pattern.compile("\\:");

    /**
     * Reusable Jackson JSON mapper
     */
    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.getDeserializationConfig().without(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);

    }

    /**
     * Simple ASCII protocol: [SERVICE_NAME][SPACE][MESSAGE_PAYLOAD]
     */
    @Override
    public RequestMessage fromTextRequest(String textRequest) {
        // try {
        String[] requestTokens = PATTERN_TEXT_REQUEST_SPLITTER.split(textRequest);
        if (requestTokens.length == 2) {
            RequestMessage requestMessage = new SimpleRequestMessage();
            requestMessage.setTargetService(requestTokens[0]);
            requestMessage.setBody(requestTokens[1]);
            return requestMessage;
        } else {
            logger.warn("Bad message:  message='{}'", textRequest);
        }
        // } catch (UnsupportedEncodingException e) {
        // logger.error("Error trying to parse request message:  " + e, e);
        // }
        return null;
    }

    @Override
    public RequestMessage fromBinaryRequest(byte[] bytes) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String toTextResponse(ResponseMessage responseMessage) {
        try {
            return OBJECT_MAPPER.writeValueAsString(responseMessage.getBody());
        } catch (Exception e) {
            logger.error("Error trying to encode response message:  " + e, e);
        }
        return null;
    }

    @Override
    public byte[] toBinaryResponse(ResponseMessage responseMessage) {
        // TODO Auto-generated method stub
        return null;
    }

}
