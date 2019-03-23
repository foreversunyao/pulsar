/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.proxy.server;


import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.raw.RawMessageIdImpl;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.channel.Channel;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;


public class ParserProxyHandler {
    private ChannelHandlerContext ctx;
    private Object msg;
    private Channel outboundChannel;
    private static final int lengthFieldLength = 4;

    public ParserProxyHandler(ChannelHandlerContext ctx, Channel outboundChannel, Object msg){
        this.ctx = ctx;
        this.outboundChannel = outboundChannel;
        this.msg =msg;
        this.parseProxyMsg();
    }

    public void parseProxyMsg(){
        ByteBuf buffer = (ByteBuf)(this.msg);
        PulsarApi.BaseCommand cmd = null;
        PulsarApi.BaseCommand.Builder cmdBuilder = null;

        MessageMetadata msgMetadata = null;
        try {
            //
            buffer.markReaderIndex();
            buffer.markWriterIndex();

            //skip lengthFieldLength
            buffer.readerIndex(ParserProxyHandler.lengthFieldLength);

            int cmdSize = (int) buffer.readUnsignedInt();
            int writerIndex = buffer.writerIndex();
            buffer.writerIndex(buffer.readerIndex() + cmdSize);
            ByteBufCodedInputStream cmdInputStream = ByteBufCodedInputStream.get(buffer);

            cmdBuilder = PulsarApi.BaseCommand.newBuilder();
            cmd = cmdBuilder.mergeFrom(cmdInputStream, null).build();
            buffer.writerIndex(writerIndex);
            //cmdInputStream.recycle();
            System.out.println("type:"+cmd.getType());
            switch (cmd.getType()) {
                case PRODUCER:
                    System.out.println(".....producer name and topic:" + cmd.getProducer().getProducerName() + cmd.getProducer().getTopic());
                    break;
                case SEND:
                    //ByteBuf headersAndPayload = buffer.markReaderIndex();

                    //msgMetadata = Commands.parseMessageMetadata(buffer);


                    //System.out.println(".....send:" + cmd.getSend().getSequenceId()+cmd.getSend().getNumMessages()+msgMetadata.getCompression()+msgMetadata.getPublishTime());
                    //ByteBuf headersAndPayload_new = headersAndPayload.retainedSlice();

                    break;
                case SUBSCRIBE:
                    System.out.println(".....consumer name"+cmd.getSubscribe().getConsumerName()+cmd.getSubscribe().getTopic());
                    break;
                case FLOW:
                    System.out.println("...flow"+cmd.getFlow().toByteString());
                    break;

            }
        } catch (Exception e){
            log.error("{}",e.getMessage());
            System.out.println(e.getMessage());
        }finally {
            if (cmdBuilder != null) {
                cmdBuilder.recycle();
            }

            if (cmd != null) {
                cmd.recycle();
            }
            buffer.resetReaderIndex();
            buffer.resetWriterIndex();
        }
        log.info("{}#{}#{}#{}#{}",ctx.channel().remoteAddress(),ctx.channel().localAddress(),outboundChannel.localAddress(),outboundChannel.remoteAddress(),"s");
    }




    private static final Logger log = LoggerFactory.getLogger(ParserProxyHandler.class);
}
