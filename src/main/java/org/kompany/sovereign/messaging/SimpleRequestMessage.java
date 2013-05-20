package org.kompany.sovereign.messaging;

/**
 * 
 * @author ypai
 * 
 */
public class SimpleRequestMessage extends AbstractMessage implements RequestMessage {

    private String targetService;

    public SimpleRequestMessage() {

    }

    public SimpleRequestMessage(String targetService, Object body) {
        setTargetService(targetService);
        setBody(body);
    }

    @Override
    public String getTargetService() {
        return targetService;
    }

    @Override
    public void setTargetService(String targetService) {
        this.targetService = targetService;
    }

}
