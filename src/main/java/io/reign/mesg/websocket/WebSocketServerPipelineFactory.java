package io.reign.mesg.websocket;

import static org.jboss.netty.channel.Channels.pipeline;
import io.reign.ReignContext;
import io.reign.mesg.MessageProtocol;

import java.util.concurrent.ExecutorService;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

/**
 */
public class WebSocketServerPipelineFactory implements ChannelPipelineFactory {

    private final ReignContext serviceDirectory;
    private final MessageProtocol messageProtocol;
    private final WebSocketConnectionManager connectionManager;
    private final ExecutorService requestMonitoringExecutor;

    public WebSocketServerPipelineFactory(ReignContext serviceDirectory, WebSocketConnectionManager connectionManager,
            MessageProtocol messageProtocol, ExecutorService requestMonitoringExecutor) {
        this.serviceDirectory = serviceDirectory;
        this.connectionManager = connectionManager;
        this.messageProtocol = messageProtocol;
        this.requestMonitoringExecutor = requestMonitoringExecutor;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(65536));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("handler", new WebSocketServerHandler(serviceDirectory, connectionManager, messageProtocol,
                requestMonitoringExecutor));
        return pipeline;
    }
}