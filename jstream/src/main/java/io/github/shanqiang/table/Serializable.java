package io.github.shanqiang.table;

public interface Serializable {
    long serializeSize();
    void serialize(byte[] bytes, long offset, long length);
    void deserialize(byte[] bytes, long offset, long length);
}
