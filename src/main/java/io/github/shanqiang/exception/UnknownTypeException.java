package io.github.shanqiang.exception;

import javax.annotation.Nonnull;

public class UnknownTypeException extends RuntimeException {
    public UnknownTypeException(@Nonnull String message) {
        super(message);
    }
}
