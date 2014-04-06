package io.reign.mesg;

import io.reign.util.JacksonUtil;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class DefaultMessageProtocol implements MessageProtocol {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageProtocol.class);

    // private static final Pattern PATTERN_TEXT_REQUEST_SPLITTER = Pattern.compile("\\:");

    public static final String MESSAGE_ID_DELIMITER = ">";

    /**
     * Reusable Jackson JSON mapper
     */
    private static ObjectMapper OBJECT_MAPPER = JacksonUtil.getObjectMapper();

    /**
     * Simple ASCII protocol: [SERVICE_NAME][COLON][MESSAGE_PAYLOAD]
     */
    @Override
    public RequestMessage fromTextRequest(String textRequest) {
        try {
            String[] requestTokens = null;
            int colonIndex = textRequest.indexOf(':');
            if (colonIndex != -1) {
                requestTokens = new String[2];
                requestTokens[0] = textRequest.substring(0, colonIndex);
                requestTokens[1] = textRequest.substring(colonIndex + 1);

            }
            if (requestTokens != null) {
                RequestMessage requestMessage = new SimpleRequestMessage();
                requestMessage.setTargetService(requestTokens[0]);

                int messageIdDelimiterIndex = requestTokens[1].lastIndexOf(MESSAGE_ID_DELIMITER);
                if (messageIdDelimiterIndex == -1) {
                    requestMessage.setBody(requestTokens[1]);
                } else {
                    int newlineIndex = requestTokens[1].indexOf("\n", messageIdDelimiterIndex + 1);
                    if (newlineIndex == -1) {
                        newlineIndex = requestTokens[1].length();
                    }
                    requestMessage.setBody(requestTokens[1].substring(0, messageIdDelimiterIndex).trim()
                            + requestTokens[1].substring(newlineIndex));
                    requestMessage.setId(Integer.parseInt(requestTokens[1].substring(messageIdDelimiterIndex + 1,
                            newlineIndex).trim()));
                }
                return requestMessage;
            } else {
                logger.warn("Poorly formatted message:  message='{}'", textRequest);
            }
        } catch (Exception e) {
            logger.error("Error trying to parse request message:  " + e, e);
        }
        return null;
    }

    @Override
    public RequestMessage fromBinaryRequest(byte[] bytes) {
        throw new UnsupportedOperationException("Not yet supported.");
    }

    @Override
    public String toTextResponse(ResponseMessage responseMessage) {
        try {
            // Map<String, Object> responseMap = new HashMap<String, Object>(2);
            // responseMap.put("status", getResponseStatusCode(responseMessage.getStatus()));
            // responseMap.put("body", responseMessage.getBody());
            return OBJECT_MAPPER.writeValueAsString(responseMessage);
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
            return OBJECT_MAPPER.readValue(textResponse, new TypeReference<SimpleResponseMessage>() {
            });
        } catch (Exception e) {
            logger.error("" + e, e);
            ResponseMessage responseMessage = new SimpleResponseMessage(ResponseStatus.ERROR_UNEXPECTED);
            responseMessage.setComment("" + e);
            return responseMessage;
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
}
