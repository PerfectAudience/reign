package org.kompany.sovereign;

/**
 * 
 * @author ypai
 * 
 */
public class SovereignException extends Exception {
    public SovereignException() {
        super();
    }

    public SovereignException(String message) {
        super(message);
    }

    public SovereignException(String message, Throwable cause) {
        super(message, cause);
    }

    public SovereignException(Throwable cause) {
        super(cause);
    }
}
