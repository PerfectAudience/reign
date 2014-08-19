package io.reign.mesg;

import io.reign.NodeInfo;

/**
 * 
 * @author ypai
 * 
 */
public interface RequestMessage extends Message {

	public String getTargetService();

	public RequestMessage setTargetService(String targetService);

	/**
	 * @return the sender of the request
	 */
	public NodeInfo getSenderInfo();

	public RequestMessage setSenderInfo(NodeInfo senderInfo);
}
