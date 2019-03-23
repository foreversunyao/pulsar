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
        String info = "";
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
            cmdInputStream.recycle();

            switch (cmd.getType()) {
                case PRODUCER:
                    info = " producer:"+cmd.getProducer().getProducerName()+" topic:"+cmd.getProducer().getTopic();

                    break;
                case SEND:
                    //ByteBuf headersAndPayload = buffer.markReaderIndex();

                    msgMetadata = Commands.parseMessageMetadata(buffer);
                    info = "sequenceid:"+msgMetadata.getSequenceId()+" encrpted:"+msgMetadata.getEncryptionKeysCount()+ " timecost:"+(System.currentTimeMillis()-msgMetadata.getPublishTime());

                    //ByteBuf headersAndPayload_new = headersAndPayload.retainedSlice();

                    break;
                case SUBSCRIBE:

                    info = " consumer:"+cmd.getSubscribe().getConsumerName()+" topic:"+cmd.getSubscribe().getTopic();

                    break;
                case FLOW:
                    //msgMetadata = Commands.parseMessageMetadata(buffer);

                    info = " producer:"+cmd.getProducer().getProducerName()+" topic:"+cmd.getProducer().getTopic();
                    break;
            }
            System.out.println(cmd.getType());
            log.info("cr:{} pi:{} po:{} pr:{} cmd:{} info:{}",ctx.channel().remoteAddress(),ctx.channel().localAddress(),outboundChannel.localAddress(),outboundChannel.remoteAddress(),cmd.getType(),info);

        } catch (Exception e){
            log.error("{}",e.getMessage());
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

    }




    private static final Logger log = LoggerFactory.getLogger(ParserProxyHandler.class);
}
