package com.creditease.dbus.rabbitmq.forward.exception;

public class RabbitmqForwardException extends RuntimeException{
    public RabbitmqForwardException() {
    }

    public RabbitmqForwardException(String message) {
        super(message);
    }

    public RabbitmqForwardException(String message, Throwable cause) {
        super(message, cause);
    }

    public RabbitmqForwardException(Throwable cause) {
        super(cause);
    }

    public RabbitmqForwardException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
