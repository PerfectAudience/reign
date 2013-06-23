package io.reign;

/**
 * 
 * @author ypai
 * 
 */
public class UnexpectedReignException extends RuntimeException {
    public UnexpectedReignException() {
        super();
    }

    public UnexpectedReignException(String message) {
        super(message);
    }

    public UnexpectedReignException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnexpectedReignException(Throwable cause) {
        super(cause);
    }
}
