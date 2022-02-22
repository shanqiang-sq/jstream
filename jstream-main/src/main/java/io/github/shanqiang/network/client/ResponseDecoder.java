package io.github.shanqiang.network.client;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class ResponseDecoder extends LengthFieldBasedFrameDecoder {
    public ResponseDecoder() {
        super(Integer.MAX_VALUE, 0, RequestEncoder.LENGTH_FIELD_LENGTH, 0, RequestEncoder.LENGTH_FIELD_LENGTH, true);
    }
}
