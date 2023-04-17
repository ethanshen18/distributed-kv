package com.g12.CPEN431.A12;

public class InsufficientResourcesException extends RuntimeException{
    public InsufficientResourcesException() {
        super();
    }
    public InsufficientResourcesException(String message) {
        super(message);
    }
    public InsufficientResourcesException(String message, Throwable cause) {
        super(message, cause);
    }
    public InsufficientResourcesException(Throwable cause) {
        super(cause);
    }
}
