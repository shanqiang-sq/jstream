package io.github.shanqiang.exception;

import javax.annotation.Nonnull;

public class IllegalSizeException extends RuntimeException {
    public IllegalSizeException(@Nonnull String message) {
        super(message);
    }
}
