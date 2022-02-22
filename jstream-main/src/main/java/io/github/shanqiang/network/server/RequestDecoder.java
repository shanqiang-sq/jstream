package io.github.shanqiang.network.server;

import io.github.shanqiang.network.client.RequestEncoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class RequestDecoder extends LengthFieldBasedFrameDecoder {
    public RequestDecoder() {
        super(Integer.MAX_VALUE, 0, RequestEncoder.LENGTH_FIELD_LENGTH, 0, RequestEncoder.LENGTH_FIELD_LENGTH, true);
    }
}
