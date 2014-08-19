package io.reign.mesg;

import io.reign.NodeInfo;
import io.reign.util.JacksonUtil;

import java.util.List;

import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author ypai
 * 
 */
public class ParsedRequestMessage implements RequestMessage {

	private static final Logger logger = LoggerFactory.getLogger(ParsedRequestMessage.class);

	private final RequestMessage mesg;

	private final String meta;
	private final String resource;
	private final QueryStringDecoder queryStringDecoder;

	public ParsedRequestMessage(RequestMessage mesg) {
		this.mesg = mesg;

		// parse and populate useful fields
		String body = (String) mesg.getBody();
		int firstHashIndex = body.indexOf("#");
		int firstNewlineIndex = body.indexOf("\n");
		int firstQuestionMarkIndex = body.indexOf("?");

		if (firstNewlineIndex == -1 || (firstQuestionMarkIndex > 0 && firstQuestionMarkIndex < firstNewlineIndex)) {
			if (firstHashIndex == -1 || (firstNewlineIndex > 0 && firstHashIndex > firstNewlineIndex)) {
				queryStringDecoder = new QueryStringDecoder(firstNewlineIndex == -1 ? body : body.substring(0,
				        firstNewlineIndex));
			} else {
				queryStringDecoder = new QueryStringDecoder(body.substring(0, firstHashIndex));
			}
		} else {
			queryStringDecoder = null;
		}

		if (firstHashIndex < 0) {
			if (firstQuestionMarkIndex > 0 && (firstNewlineIndex == -1 || firstQuestionMarkIndex < firstNewlineIndex)) {
				resource = body.substring(0, firstQuestionMarkIndex).trim();

			} else {
				if (firstNewlineIndex < 0) {
					resource = body.trim();
				} else {
					resource = body.substring(0, firstNewlineIndex).trim();
				}
			}
			meta = null;

		} else {
			if (firstQuestionMarkIndex > 0 && firstQuestionMarkIndex < firstHashIndex) {
				resource = body.substring(0, firstQuestionMarkIndex).trim();
			} else {
				resource = body.substring(0, firstHashIndex).trim();
			}

			if (firstNewlineIndex < 0) {
				meta = body.substring(firstHashIndex + 1).trim();
			} else {
				meta = body.substring(firstHashIndex + 1, firstNewlineIndex).trim();
			}
		}

	}

	public String getMeta() {
		if (meta != null) {
			return meta;
		} else {
			return getQueryParameterValue("meta");
		}
	}

	public String getQueryParameterValue(String parameterName) {
		if (this.queryStringDecoder != null) {
			List<String> valueList = this.queryStringDecoder.getParameters().get(parameterName);
			if (valueList != null) {
				return valueList.get(0);
			}
		}
		return null;

	}

	public List<String> getQueryParameterValues(String parameterName) {
		if (this.queryStringDecoder != null) {
			return this.queryStringDecoder.getParameters().get(parameterName);
		} else {
			return null;
		}
	}

	public String getResource() {
		return resource;
	}

	@Override
	public NodeInfo getSenderInfo() {
		return mesg.getSenderInfo();
	}

	@Override
	public RequestMessage setSenderInfo(NodeInfo senderInfo) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer getId() {
		return mesg.getId();
	}

	@Override
	public Message setId(Integer id) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getTargetService() {
		return mesg.getTargetService();
	}

	@Override
	public RequestMessage setTargetService(String targetService) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getBody() {
		return mesg.getBody();
	}

	@Override
	public Message setBody(Object body) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		try {
			return JacksonUtil.getObjectMapper().writeValueAsString(this);
		} catch (Exception e) {
			logger.error("" + e, e);
			return super.toString();
		}
	}

}
