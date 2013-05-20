package org.kompany.sovereign.messaging;

/**
 * 
 * @author ypai
 * 
 */
public interface MessageProtocol {

    /**
     * 
     * @param asciiRequest
     *            UTF-8 assumed
     * @return
     */
    public RequestMessage fromTextRequest(String textRequest);

    /**
     * 
     * @param bytes
     * @return
     */
    public RequestMessage fromBinaryRequest(byte[] bytes);

    /**
     * 
     * @param responseMessage
     * @return UTF-8 String
     */
    public String toTextResponse(ResponseMessage responseMessage);

    /**
     * 
     * @param responseMessage
     * @return
     */
    public byte[] toBinaryResponse(ResponseMessage responseMessage);

}
