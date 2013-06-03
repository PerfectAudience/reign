package org.kompany.sovereign;

/**
 * 
 * @author ypai
 * 
 */
public class UnexpectedSovereignException extends RuntimeException {
    public UnexpectedSovereignException() {
        super();
    }

    public UnexpectedSovereignException(String message) {
        super(message);
    }

    public UnexpectedSovereignException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnexpectedSovereignException(Throwable cause) {
        super(cause);
    }
}
