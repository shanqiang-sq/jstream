package io.github.shanqiang.exception;

import javax.annotation.Nonnull;

public class UnknownCommandException extends RuntimeException {
    public UnknownCommandException(@Nonnull String message) {
        super(message);
    }
}
