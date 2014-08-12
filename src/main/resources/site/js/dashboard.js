var connectWebSocket;
$(function() {	
	
	var socket;
	var requestIdSequence = 0;	
	
	$('#service-list').on('show.bs.collapse', function() {
		$('#service-list-label .glyphicon').addClass('glyphicon-chevron-down').removeClass('glyphicon-chevron-left');
	});
	$('#service-list').on('hide.bs.collapse', function() {
		$('#service-list-label .glyphicon').removeClass('glyphicon-chevron-down').addClass('glyphicon-chevron-left');
	});	
	
	$('#coordination-list').on('show.bs.collapse', function() {
		$('#coordination-list-label .glyphicon').addClass('glyphicon-chevron-down').removeClass('glyphicon-chevron-left');
	});	
	$('#coordination-list').on('hide.bs.collapse', function() {
		$('#coordination-list-label .glyphicon').removeClass('glyphicon-chevron-down').addClass('glyphicon-chevron-left');
	});
	
    $("#cluster-id-menu .dropdown-menu").on('click', 'li a', function() {
    	// handle selection of cluster
    	
    	var previousClusterId = selectedClusterId();
    	hideAllClusterData();
    	hideAllServiceData();     	
    	
    	var clusterId = $(this).text().trim();
    	//console.log(clusterId);
    	$("#cluster-id").text(clusterId);
    	$("#cluster-id").attr("clusterId", clusterId);   	
    	
     	$("li.active a.cluster-service-info").parent().removeClass("active");
    	
     	send("presence:/"+previousClusterId+"#observe-stop > 1");
     	
    	send("presence:/"+clusterId+" > 1");
    	send("presence:/"+clusterId+"#observe > 1");   
    	
//    	$("#coordination-list-label").removeClass('hidden');
//    	$('#coordination-list').removeClass('hidden');
    	
    	$('.nav-sidebar-divider').removeClass('hidden');
    	
    });
    
    $("#service-list").on('click', 'li a.cluster-service-info', function() {
    	// handle selection of service    	
    	hideAllServiceData();
    	
    	var clusterId = $("#cluster-id").text().trim();
    	//var serviceId = $(this).text().trim();
    	var serviceId = $(this).attr("serviceId").trim();
    	var clusterIdServiceId = clusterId+"/"+serviceId;
    	//console.log(clusterIdServiceId);
    	
    	var previousServiceId = $("li.active a.cluster-service-info").attr("serviceId");
    	
    	$(".service-name").html(clusterId+"/"+serviceId+'&nbsp;');
    	
    	$("a.cluster-service-info").parent().removeClass("active");
    	$(this).parent().addClass("active");
    	
    	send("presence:/"+clusterId+"/"+previousServiceId+"#observe-stop > 5");
    	send("metrics:/"+clusterId+"/"+previousServiceId+"#observe-stop > 5");
    	
    	send("presence:/"+clusterIdServiceId+" > 4");
    	send("presence:/"+clusterIdServiceId+"#observe > 5");
    	
    	send("metrics:/"+clusterIdServiceId+" > 2");
    	send("metrics:/"+clusterIdServiceId+"#observe > 5");
    });    
    
    $("#coordination-list").on('click', 'li a', function() {
    	// handle selection of coordination data    	
    	hideAllCoordinationData();    	   
    	
    	var clusterId = $("#cluster-id").text().trim();
    	var coordEntity = $(this).attr("coordEntity").trim();
    	
    	location.hash = clusterId+"/"+coordEntity;
    	
    	console.log("Sending:  coord:/"+clusterId+"/"+coordEntity+" > 100");    	
    	
    	send("coord:/"+clusterId+"/"+coordEntity+" > 100");
    });      

    function hideAllCoordinationData() {
    	
    }
    
	if (!window.WebSocket) {
		window.WebSocket = window.MozWebSocket;
	}

	function send(message) {
		if (!window.WebSocket) {
			return;
		}			
		var requestId = 0;
		var idDelimiterIndex = message.indexOf(">");
		if (idDelimiterIndex < 0) {
			requestId = 0;
			message = message + " > " + requestIdSequence;
		} else {
			var newlineIndex = message.indexOf("\n", idDelimiterIndex);
			if( newlineIndex < 0 ) {
				requestId = message.substring(idDelimiterIndex+1);
		    } else {
		    	requestId = message.substring(idDelimiterIndex+1, newlineIndex);
		    }
			requestId = requestId.trim();
		}
		if( !socket || socket.readyState==socket.CLOSED ) {
			connectWebSocket($('#connectHost').val());	
			
			// add additional event handling
			var currentOnOpen = socket.onopen;
			socket.onopen = function(event) {
				currentOnOpen(event);
				handleRequest(message);
				socket.send(message);
			};
			return;
		}
		if ( socket.readyState==WebSocket.CLOSED || socket.readyState==WebSocket.CLOSING) {							
			handleErrorMessage(
					'Web Socket connection error:&nbsp; '
							+ socket.url);				
		} else {
			handleRequest(message, requestId);
			socket.send(message);
		}
	} // send

	connectWebSocket = function ( uri ) {			
		// close socket if already open
		if (socket && socket.readyState!=socket.CLOSED && socket.readyState!=socket.CLOSING ) {
			socket.close();
			socket = null;
	    }
		
		// open web socket if possible in browser
		if (window.WebSocket) {
			var socketUri = uri;
			var newSocket = null;
			try {
				newSocket = new WebSocket(socketUri);
			} catch( err ) {
				handleErrorMessage(
						'Web Socket connection error:&nbsp; '
								+ err.message);
				return;
			}
			newSocket.onmessage = function(event) {
				var statusIndex = -1;
				var id = 0;
				if (event.data) {
					statusIndex = event.data.indexOf("status");
				}
				if (statusIndex < 0) {
					handleIncomingMessage(event.data);
				} else {
					handleResponse(event.data);
				}
			};
			newSocket.onopen = function(event) {
				handleControlMessage('Web Socket opened:  '
						+ socketUri);
				
				send('presence:/#observe > 5');
				send('presence:/ > 6');				
			};
			newSocket.onclose = function(event) {
				handleControlMessage(
						'Web Socket closed:&nbsp; '
								+ socketUri);
			};
			newSocket.onerror = function(event) {
				handleErrorMessage(
						'Web Socket connection error:&nbsp; '
								+ socketUri);
			};
			socket = newSocket;
		} else {
			alert("Your browser does not support Web Socket.");
		}
		
	} // connectWebSocket

	function handleResponse(html) {
		//console.log(html);

		var response = eval('(' + html + ')');
		if( response.body ) {
			//console.log("non-event:  "+html);
			if( (response.id && response.id==3) ) {
				// service node count
				var nodeIdList = response.body.nodeIdList;
				var clusterId = response.body.clusterId;
				var serviceId = response.body.serviceId;
				var clusterIdServiceId = clusterId+'-'+serviceId;
				$('#'+clusterIdServiceId+'-node-count').html(nodeIdList.length);
				
			} else if( (response.id && response.id==2) ) {
				// metrics update
				renderMetrics( response.body.clusterId, response.body.serviceId, response.body );
                
		    } else if( (response.id && response.id==4) ) {
		    	// service node update
                renderNodeList(response.body.clusterId, response.body.serviceId, response.body.nodeIdList);
                updateServiceNodeCount(response.body.clusterId, response.body.serviceId, response.body.nodeIdList.length);
	
		    } else if( (response.id && response.id==1) ) {		    
				// service list
				var serviceList  = response.body;
				serviceList.sort();
				var serviceHtml = '';
				var clusterId = $("#cluster-id").text().trim();
				for (var i = 0; i < serviceList.length; i++) { 
				  var clusterIdServiceId = clusterId+'-'+serviceList[i];
				  serviceHtml += '<li><a id="'+clusterIdServiceId+'-info" class="cluster-service-info" href="#'+clusterId+"/"+serviceList[i]+'" serviceId="'+serviceList[i]+'"><span id="'+clusterIdServiceId+'-id" class="cluster-service-id">'+serviceList[i]+'</span>&nbsp;<span id="'+clusterIdServiceId+'-node-count" class="cluster-service-node-count badge alert-success pull-right"></span></a></li>';
				  send("presence:/"+clusterId+"/"+serviceList[i] + " > 3");
				  send("presence:/"+clusterId+"/"+serviceList[i] + "#observe > 5");
				}
				
				$('#service-list').html(serviceHtml);
				
				if( serviceList.length>0 ) {
					$('#service-list-label').removeClass('hidden');
				    $('#service-list').collapse('show');
		        } else {
		        	$('#service-list-label').addClass('hidden');
		        	$('#service-list').collapse('hide');
		        }
				
				// if there is a hash location, attempt to show it
				handleHash('service');

			} else if( (response.id && response.id==6) ) {
				// cluster list
				var clusterList  = response.body;
				clusterList.sort();
				var clusterListHtml = '';
				for( var i=0; i<clusterList.length; i++) {
					if( i>0 ) {
						clusterListHtml += '<li class="divider"></li>';
					}
					clusterListHtml += '<li><a id="'+clusterList[i]+'-info" href="#'+clusterList[i]+'">'+clusterList[i]+'</a></li>';
				}
				$('#cluster-id-menu-items').html(clusterListHtml);
				$('#service-list').html(serviceHtml);
				
				// if there is a hash location, attempt to show it
				handleHash('cluster');
				
			} else if(response.id && response.id==100) {
				console.log("Coord:  "+html);
				
				var lockList  = response.body;
				lockList.sort();
				
				var html = '';
				var clusterId = $("#cluster-id").text().trim();
				for (var i = 0; i < lockList.length; i++) { 
				    html += '<tr><td><a href="#'+(clusterId+'/lock/'+lockList[i])+'">'+lockList[i]+'</a></td></tr>';
				}
				
				console.log(html);
				$('#lock-list-data').html(html);
				
				if( lockList.length>0 ) {
					$('#lock-list').removeClass('hidden');
				}
			}
			
		} else if( response.event ) {
			//console.log("event:  "+html);
		}		
	}
	
	function selectedServiceId() {
		return $("li.active a.cluster-service-info").attr("serviceId");	
	}
	
	function selectedClusterId() {
		return $("#cluster-id").attr("clusterId");
	}
	
	
	function renderNodeList(clusterId, serviceId, nodeIdList) {
		if( clusterId!=selectedClusterId() ) {			
			send("presence:/"+clusterId+"/"+serviceId+"#observe-stop > 5");
		}
		if( serviceId!=selectedServiceId() ) {
			return;
		}
		
    	        // service node list
		var serviceNodeHtml = '';
		
		// sort node list
		nodeIdList.sort(function(a, b) {
	        if (a.h < b.h) {
	            return -1;
	        }  else if (a.h > b.h) {
	            return 1;
		    }
	        return 0;
	    });
     
    	if( nodeIdList.length && nodeIdList.length>0 ) {
            for (var i = 0; i < nodeIdList.length; i++) { 
              var nodeInfo = nodeIdList[i];
              var pid = nodeInfo.pid ? nodeInfo.pid : '--';
              serviceNodeHtml += '<tr><td>'+pid+'</td><td>'+nodeInfo.h+'</td><td>'+nodeInfo.ip+'</td><td>'+nodeInfo.mp+'</td></tr>';
            }
            //console.log(serviceNodeHtml);
        } else {
        	serviceNodeHtml = '<tr><td colspan="4">No nodes available.</td></tr>'
        }
    	
        $('#service-node-list-data').html(serviceNodeHtml);
        $('#service-node-list').fadeIn('fast').removeClass('hidden');
	}
	
	function updateServiceNodeCount(clusterId, serviceId, count) {
		$('#'+clusterId+"-"+serviceId+'-node-count').html(count);
	}
	
    function sortedKeyList( map ) {
        var keyList = new Array();
        var i = 0;
        for(var key in map) {
            keyList[i] = key;
            i++;
        }
        keyList.sort(function(a, b) {
	        if (a < b) {
	            return -1;
	        }  else if (a > b) {
	            return 1;
		    }
	        return 0;
	    });
        return keyList;
    }
    
	function renderMetrics( clusterId, serviceId, metrics ) {
		if( !clusterId || clusterId!=selectedClusterId() ) {			
			send("metrics:/"+clusterId+"/"+serviceId+"#observe-stop > 5");
		}
		if( !serviceId || serviceId!=selectedServiceId() ) {
			return;
		}
		
		// counters
		if( metrics.counters ) {
			var counters = metrics.counters;
			var counterHtml = '';
			var count = 0;
            var metricKeyList = sortedKeyList(counters);
			for(var i = 0; i < metricKeyList.length; i++) {		
              var counterName = metricKeyList[i];
			  var counter = counters[counterName];
              counterHtml += '<tr id="'+counterName+'" class="metric-data">';
              counterHtml += '<td>'+counterName+'</td>';
              counterHtml += '<td>'+counter.count+'</td>';
              counterHtml += '</tr>';
              count++;
			}
			$('#metrics-counter-list-data').html(counterHtml);				
			if( count>0 ) {
              $('#metrics-counter-list').fadeIn('fast').removeClass('hidden');
		    }
		}// counters			
		
		// histograms
		if( metrics.histograms ) {
			var histograms = metrics.histograms;
			var histogramHtml = '';
			var count = 0;
            var metricKeyList = sortedKeyList(histograms);
			for(var i = 0; i < metricKeyList.length; i++) {		
              var histogramName = metricKeyList[i];
			  var histogram = histograms[histogramName];
			  histogramHtml += '<tr id="'+histogramName+'" class="metric-data">';
			  histogramHtml += '<td>'+histogramName+'</td>';
		      histogramHtml += '<td>'+Math.round(histogram.count)+'</td>';					  
		      histogramHtml += '<td>'+Math.round(histogram.max)+'</td>';
		      histogramHtml += '<td>'+Math.round(histogram.mean)+'</td>';
		      histogramHtml += '<td>'+Math.round(histogram.min)+'</td>';
		      histogramHtml += '<td>'+Math.round(histogram.p50)+'</td>';
              histogramHtml += '<td>'+Math.round(histogram.p75)+'</td>';
		      histogramHtml += '<td>'+Math.round(histogram.p95)+'</td>';
		      histogramHtml += '<td>'+Math.round(histogram.p98)+'</td>';
		      histogramHtml += '<td>'+Math.round(histogram.p99)+'</td>';
		      histogramHtml += '<td>'+Math.round(histogram.p999)+'</td>';
		      histogramHtml += '</tr>';
		      count++;
			}
			if( count>0 ) {
			  $('#metrics-histogram-list-data').html(histogramHtml);
              $('#metrics-histogram-list').fadeIn('fast').removeClass('hidden');
			}
		}// histograms
		
		// meters
		if( metrics.meters ) {
			var meters = metrics.meters;
			var meterHtml = '';
			var count = 0;
            var metricKeyList = sortedKeyList(meters);
			for(var i = 0; i < metricKeyList.length; i++) {		
              var meterName = metricKeyList[i];
			  var meter = meters[meterName];
			  meterHtml += '<tr id="'+meterName+'" class="metric-data">';
			  meterHtml += '<td>'+meterName+'</td>';
			  meterHtml += '<td>'+Math.round(meter.count)+'</td>';					  
			  meterHtml += '<td>'+Math.round(meter.meanRate)+'</td>';
			  meterHtml += '<td>'+Math.round(meter.m1Rate)+'</td>';
			  meterHtml += '<td>'+Math.round(meter.m5Rate)+'</td>';
			  meterHtml += '<td>'+Math.round(meter.m15Rate)+'</td>';
			  meterHtml += '</tr>';
		      count++;
			}
			if( count>0 ) {
			  $('#metrics-meter-list-data').html(meterHtml);
              $('#metrics-meter-list').fadeIn('fast').removeClass('hidden');
			}
		}// meters	
		
		// timers
		if( metrics.timers ) {
			var timers = metrics.timers;
			var timerHtml = '';
			var count = 0;
            var metricKeyList = sortedKeyList(timers);
			for(var i = 0; i < metricKeyList.length; i++) {		
              var timerName = metricKeyList[i];
			  var timer = timers[timerName];
			  timerHtml += '<tr id="'+timerName+'" class="metric-data">';
			  timerHtml += '<td>'+timerName+'</td>';
			  timerHtml += '<td>'+Math.round(timer.count)+'</td>';					  
			  timerHtml += '<td>'+Math.round(timer.max)+'</td>';
			  timerHtml += '<td>'+Math.round(timer.mean)+'</td>';
			  timerHtml += '<td>'+Math.round(timer.min)+'</td>';
			  timerHtml += '<td>'+Math.round(timer.p50)+'</td>';
              timerHtml += '<td>'+Math.round(timer.p75)+'</td>';
			  timerHtml += '<td>'+Math.round(timer.p95)+'</td>';
			  timerHtml += '<td>'+Math.round(timer.p98)+'</td>';
			  timerHtml += '<td>'+Math.round(timer.p99)+'</td>';
			  timerHtml += '<td>'+Math.round(timer.p999)+'</td>';
			  timerHtml += '</tr>';
		      count++;
			}
			if( count>0 ) {
			  $('#metrics-timer-list-data').html(timerHtml);
              $('#metrics-timer-list').fadeIn('fast').removeClass('hidden');
			}
		}// timers
	}

	function hideAllServiceData() {
		$('.metric-list').hide();
		$('#service-node-list').hide();
	}
	
	function hideAllClusterData() {		

	}
	
	function handleRequest(html, requestId) {
		//console.log(html);	
	}

	function handleIncomingMessage(raw) {
		//console.log("EVENT:  " + raw);
		
		var mesg = eval('(' + raw + ')');
		
		// handle event	
		if( mesg.event && mesg.event=="metrics" ) {			
			renderMetrics( mesg.clusterId, mesg.serviceId, mesg.body.updated );
		} else if(mesg.event && mesg.event=="presence") {
            renderNodeList(mesg.clusterId, mesg.serviceId, mesg.body.updated.nodeIdList);
            updateServiceNodeCount(mesg.clusterId, mesg.serviceId, mesg.body.updated.nodeIdList.length);	    
		}
	}

	function handleControlMessage(html) {
		//console.log(html);
	}

	function handleErrorMessage(html) {
		//console.log(html);
	}
	
	function handleHash( type ) {
		hash = location.hash ? location.hash.replace('#', '') : null;
		
		//console.log('handleHash():  hash='+hash);
		
		if( !hash || !type ) {
			return;
		}
		
		hash = hash.trim();
		
		// strip leading slash
		var slashIndex = hash.indexOf('/');
		if( slashIndex==0 ) {
			hash = hash.substring(slashIndex+1);			
		} 
		
		// strip trailing slash
		slashIndex = hash.lastIndexOf('/');
		if( slashIndex==hash.length-1 ) {
			hash = hash.substring(0,hash.length-1);			
		} 
		
		location.hash = hash;
		
		// split on middle slash
	    slashIndex = hash.indexOf('/');
		var clusterId = null;
		var serviceId = null;
		if( slashIndex!=-1) {
			clusterId = hash.substring(0, slashIndex);	
			serviceId = hash.substring(slashIndex+1);
		} else {
			clusterId = hash;
		}
		
		//console.log('handleHash:  type='+type+'; hash='+hash+'; clusterId='+clusterId+"; serviceId="+serviceId);
		
		if( type=='cluster' && clusterId ) {
			$('#'+clusterId+'-info').click();
		}
		if( type=='service' && serviceId ) {
			$('#'+clusterId+'-'+serviceId+'-info').click();
		}
		
	}
	
	connectWebSocket($('#connectHost').val());
	

	
	
});
