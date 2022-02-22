package io.github.shanqiang.exception;

import javax.annotation.Nonnull;

public class OutOfOrderException extends RuntimeException {
    public OutOfOrderException(@Nonnull String message) {
        super(message);
    }
}
