/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.reign.mesg.websocket;

/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.util.CharsetUtil;

/**
 * Generates Sovereign Terminal page.
 */
public final class WebSocketServerIndexPage {

    private static final String NEWLINE = "\r\n";

    public static ChannelBuffer getContent(String webSocketLocation) {
        return ChannelBuffers
                .copiedBuffer(
                        "<html><head><title>Reign :: Terminal</title></head>"
                                + NEWLINE
                                + "<body>"
                                + NEWLINE
                                + "<script type=\"text/javascript\">"
                                + NEWLINE
                                + "var socket; var socketOpened = false;"
                                + NEWLINE

                                + "if (!window.WebSocket) {"
                                + NEWLINE
                                + "  window.WebSocket = window.MozWebSocket;"
                                + NEWLINE
                                + '}'
                                + NEWLINE
                                + "function connectWebSocket() {"
                                + NEWLINE
                                + "if( socket && socket.readyState!=socket.CLOSED ) return;"
                                + NEWLINE
                                + "if (window.WebSocket) {"
                                + NEWLINE
                                + "  socket = new WebSocket(\""
                                + webSocketLocation
                                + "\");"
                                + NEWLINE
                                + "  socket.onmessage = function(event) {"
                                + "    appendDiv('responseText', event.data);"
                                + NEWLINE
                                + "  };"
                                + NEWLINE
                                + "  socket.onopen = function(event) {"
                                + "    socketOpened = true; appendDiv('responseText', 'Web Socket opened:  "
                                + webSocketLocation
                                + "');"
                                + NEWLINE
                                + "  };"
                                + NEWLINE
                                + "  socket.onclose = function(event) {"
                                + NEWLINE
                                + "    if( socketOpened ) { appendDiv('responseText', 'Web Socket closed.'); } "
                                + "    socket = null; socketOpened = false;"
                                + NEWLINE
                                + "  };"
                                + "  socket.onerror = function(event) {"
                                + NEWLINE
                                + "    if( socket ) { appendDiv('responseText', 'Web Socket connection error:&nbsp; "
                                + webSocketLocation
                                + ".'); } "
                                + "    socket = null; socketOpened = false;"
                                + NEWLINE
                                + "  };"
                                + NEWLINE
                                + "} else {"
                                + NEWLINE
                                + "  alert(\"Your browser does not support Web Socket.\");"
                                + NEWLINE
                                + '}'
                                + NEWLINE
                                + "} // function"
                                + NEWLINE
                                + NEWLINE
                                + "function send(message) {"
                                + NEWLINE
                                + "  if (!window.WebSocket) { return; }"
                                + NEWLINE
                                + "  if (!socket || socket.readyState != WebSocket.OPEN) {"
                                + NEWLINE
                                + "    connectWebSocket(); alert(\"The socket is not open:  reconnecting:  "
                                + webSocketLocation
                                + "\"); setTimeout( function(){ if (socket && socket.readyState == WebSocket.OPEN) {socket.send(message);} }, 200);"
                                + NEWLINE
                                + "  } else { socket.send(message); }"
                                + NEWLINE
                                + "} // function"
                                + NEWLINE
                                + "function appendDiv(divElementId, html ) { var theDiv = document.getElementById(divElementId);"
                                + NEWLINE
                                + "var content = document.createElement('p'); content.innerHTML = html;"
                                + NEWLINE
                                + "theDiv.appendChild(content); theDiv.scrollTop = theDiv.scrollHeight; }"
                                + NEWLINE
                                + NEWLINE
                                + "function checkKey(e) { if( e && e.keyCode==13) { send(document.getElementById('sendInput').value); } }"
                                + NEWLINE
                                + "// setInterval( function() { connectWebSocket(); }, 5000 );"
                                + NEWLINE
                                + "window.onload = function () { connectWebSocket(); };"
                                + NEWLINE
                                + NEWLINE
                                + "</script>"
                                + NEWLINE
                                + "<form onsubmit=\"return false;\">"
                                + NEWLINE
                                + "<input type=\"text\" id=\"sendInput\" name=\"message\" onkeypress=\"return checkKey(event);\" value=\"presence:[clusterId]/[serviceId]/[nodeId]\" style=\"width:90%;\"/>"
                                + "<input type=\"button\" value=\"Send\""
                                + NEWLINE
                                + "       onclick=\"send(this.form.message.value)\" />"
                                + NEWLINE
                                + "<h3>Output</h3>"
                                + NEWLINE
                                + "<div id=\"responseText\" style=\"width:90%;height:80%;padding:1em;border:1px solid;font-family:monospace;overflow:scroll;\"></div>"
                                + NEWLINE + "</form>" + NEWLINE + "</body>" + NEWLINE + "</html>" + NEWLINE,
                        CharsetUtil.UTF_8);
    }

    private WebSocketServerIndexPage() {
        // Unused
    }
}