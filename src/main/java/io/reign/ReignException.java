package io.reign;

/**
 * 
 * @author ypai
 * 
 */
public class ReignException extends Exception {
    public ReignException() {
        super();
    }

    public ReignException(String message) {
        super(message);
    }

    public ReignException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReignException(Throwable cause) {
        super(cause);
    }
}
