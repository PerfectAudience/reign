package org.kompany.sovereign.messaging;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.kompany.sovereign.UnexpectedSovereignException;
import org.kompany.sovereign.util.JacksonUtil;
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
    private static ObjectMapper OBJECT_MAPPER = JacksonUtil.getObjectMapperInstance();

    /**
     * Simple ASCII protocol: [SERVICE_NAME][COLON][MESSAGE_PAYLOAD]
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
        throw new UnsupportedOperationException("Not yet supported.");
    }

    @Override
    public String toTextResponse(ResponseMessage responseMessage) {
        try {
            Map<String, Object> responseMap = new HashMap<String, Object>(2);
            responseMap.put("status", getResponseStatusCode(responseMessage.getStatus()));
            responseMap.put("body", responseMessage.getBody());
            return OBJECT_MAPPER.writeValueAsString(responseMap);
        } catch (Exception e) {
            logger.error("Error trying to encode response message:  " + e, e);
        }
        return null;
    }

    @Override
    public byte[] toBinaryResponse(ResponseMessage responseMessage) {
        throw new UnsupportedOperationException("Not yet supported.");
    }

    @Override
    public ResponseMessage fromTextResponse(String textResponse) {
        try {
            return OBJECT_MAPPER.readValue(textResponse, new TypeReference<ResponseMessage>() {
            });
        } catch (Exception e) {
            throw new UnexpectedSovereignException(e);
        }
    }

    @Override
    public ResponseMessage fromBinaryResponse(byte[] bytes) {
        throw new UnsupportedOperationException("Not yet supported.");
    }

    @Override
    public String toTextRequest(RequestMessage requestMessage) {
        return requestMessage.getTargetService() + ":" + requestMessage.getBody();
    }

    @Override
    public byte[] toBinaryRequest(RequestMessage requestMessage) {
        throw new UnsupportedOperationException("Not yet supported.");
    }

    @Override
    public byte getResponseStatusCode(ResponseStatus responseStatus) {
        switch (responseStatus) {
        case OK:
            return (byte) 0;
        default:
            return (byte) 5;
        }
    }
}
