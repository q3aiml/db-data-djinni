package net.q3aiml.dbdata.cli;

public class InvalidArgumentException extends RuntimeException {
    public InvalidArgumentException(String message, Throwable cause) {
        super(message, cause);
    }
}
