/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package io.reign;

/**
 * 
 * @author ypai
 * 
 */
public class StaticNodeAddress implements NodeAddress {

	private final String nodeId;
	private final String ipAddress;
	private final String host;
	private final Integer messagingPort;

	public StaticNodeAddress(String nodeId, String ipAddress, String host, Integer messagingPort) {
		this.nodeId = nodeId;
		this.ipAddress = ipAddress;
		this.host = host;
		this.messagingPort = messagingPort;
	}

	@Override
	public String getNodeId() {
		return nodeId;
	}

	@Override
	public String getIpAddress() {
		return ipAddress;
	}

	@Override
	public String getHost() {
		return host;
	}

	@Override
	public Integer getMessagingPort() {
		return messagingPort;
	}

}
