package io.reign.mesg;

public interface MessagingProviderCallback {

    public void response(String response);

    public void response(byte[] response);

    public void error(Object object);

}
