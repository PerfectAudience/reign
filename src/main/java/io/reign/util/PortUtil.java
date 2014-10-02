package io.reign.util;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Found on Stackoverflow...
 * 
 * @author ypai
 *
 */
public class PortUtil {

	private static final Logger logger = LoggerFactory.getLogger(PortUtil.class);

	public static int getAvailablePort(int lowerPortInclusive, int upperPortInclusive) {
		for (int i = lowerPortInclusive; i <= upperPortInclusive; i++) {
			if (isAvailable(i)) {
				return i;
			}
		}

		return -1;
	}

	/**
	 * Checks to see if a specific port is available.
	 *
	 * @param port
	 *            the port to check for availability
	 */
	public static boolean isAvailable(int port) {
		ServerSocket ss = null;
		DatagramSocket ds = null;
		try {
			ss = new ServerSocket(port);
			ss.setReuseAddress(true);
			ds = new DatagramSocket(port);
			ds.setReuseAddress(true);
			return true;
		} catch (IOException e) {
		} finally {
			if (ds != null) {
				ds.close();
			}

			if (ss != null) {
				try {
					ss.close();
				} catch (IOException e) {
					/* should not be thrown */
				}
			}
		}

		return false;
	}
}
