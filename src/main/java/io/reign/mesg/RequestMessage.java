package io.reign.mesg;

import io.reign.NodeAddress;

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
	public NodeAddress getSenderInfo();

	public RequestMessage setSenderInfo(NodeAddress senderInfo);
}
