package org.apache.pulsar.proxy.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.ByteOrder;

public class MessageEncoder extends MessageToByteEncoder {


    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Object msg, ByteBuf byteBuf)  {
        System.out.println("encode...");
        byteBuf.writeBytes((ByteBuf)(msg));


    }
}
