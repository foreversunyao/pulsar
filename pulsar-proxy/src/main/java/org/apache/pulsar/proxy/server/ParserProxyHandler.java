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


import avro.shaded.com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.raw.MessageParser;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.api.raw.RawMessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.channel.Channel;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.raw.MessageParser;
import java.io.IOException;
import java.util.List;
import java.util.Base64;


public class ParserProxyHandler {
    private ChannelHandlerContext ctx;
    private Object msg;
    private Channel outboundChannel;
    private static final int lengthFieldLength = 4;
    private String topic="";
    private List<RawMessage> messages;
    private String info;
    private TopicName topicName;

    public ParserProxyHandler(){

    }
    public void setParserProxyHandler(ChannelHandlerContext ctx, Channel outboundChannel, Object msg){
        this.ctx = ctx;
        this.outboundChannel = outboundChannel;
        this.msg =msg;
        this.info="";
        this.parseProxyMsg();
    }

    private void parseProxyMsg(){
        ByteBuf buffer = (ByteBuf)(this.msg);
        PulsarApi.BaseCommand cmd = null;
        PulsarApi.BaseCommand.Builder cmdBuilder = null;

        //MessageMetadata msgMetadata = null;

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
                    info = " {producer:"+cmd.getProducer().getProducerName()+",topic:"+cmd.getProducer().getTopic()+"}";
                    this.topic=cmd.getProducer().getTopic();
                    System.out.println("producer"+this.hashCode()));
                    break;
                case SEND:
                    messages = Lists.newArrayList();
                    topicName = TopicName.get(this.topic);

                    MessageParser.parseMessage(topicName,  -1L,
                            -1L,buffer,(message) -> {
                                messages.add(message);
                            });
                    for (int i=0;i <messages.size();i++){
                        System.out.println("messageSend:"+  new String(ByteBufUtil.getBytes((messages.get(i)).getData()),"UTF8"));
                    }
                    System.out.println("send"+this.hashCode());
                    break;
                case SUBSCRIBE:
                    info = "{consumer:"+cmd.getSubscribe().getConsumerName()+",topic:"+cmd.getSubscribe().getTopic()+"}";
                    this.topic = cmd.getSubscribe().getTopic();
                    topicName = TopicName.get(this.topic);
                    System.out.println("subscrbie:"+this.hashCode());
                    break;
                case SUCCESS:
                    System.out.println("success:"+this.getClass());
                    info = "success:"+cmd.getSuccess().getSchema().getName();
                    break;
                case MESSAGE:

                    System.out.println("message:"+this.hashCode());
                    //MessageMetadata msgMetadata = Commands.parseMessageMetadata(buffer);
                    topicName=TopicName.get(this.topic);
                    messages = Lists.newArrayList();
                    //topicName = TopicName.get("persistent://proxy-tenant/proxy-namespace/proxy-v0");

                    MessageParser.parseMessage(topicName,  -1L,
                            -1L,buffer,(message) -> {
                                messages.add(message);
                            });
                    for (int i=0;i <messages.size();i++){
                        System.out.println("messageConsume:"+  new String(ByteBufUtil.getBytes((messages.get(i)).getData()),"UTF8"));
                    }
                    info = "{consumer:"+cmd.getMessage().getConsumerId()+"}";
                    break;

            }
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
