package io.github.shanqiang.exception;

import javax.annotation.Nonnull;

public class ColumnNameConflictException extends RuntimeException {
    public ColumnNameConflictException(@Nonnull String message) {
        super(message);
    }
}
