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
    private Channel channel;
    private static final int lengthFieldLength = 4;
    private static String topic="";
    private List<RawMessage> messages;
    private String info;
    private TopicName topicName;
    private String type;
    private PulsarApi.BaseCommand cmd = null;
    private PulsarApi.BaseCommand.Builder cmdBuilder = null;

    public ParserProxyHandler(){

    }
    public synchronized void setParserProxyHandler(ChannelHandlerContext ctx, Channel channel, Object msg, String type){

        this.ctx = ctx;
        this.channel = channel;
        this.msg =msg;
        this.type=type;
        this.parseProxyMsg();

    }

    private synchronized void parseProxyMsg(){
        this.info="";
        ByteBuf buffer = (ByteBuf)(this.msg);
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
            //System.out.println(type+"#"+cmd.getType().toString()+"#"+java.lang.System.identityHashCode(this));
            switch (cmd.getType()) {
                case PRODUCER:
                    MessageMetadata msgMetadata = Commands.parseMessageMetadata(buffer);
                    info = " {producer:"+cmd.getProducer().getProducerName()+",sequenceid:"+msgMetadata.getSequenceId()+",topic:"+cmd.getProducer().getTopic()+"}";
                    ParserProxyHandler.topic=cmd.getProducer().getTopic();

                    break;
                case SEND:
                    messages = Lists.newArrayList();
                    topicName = TopicName.get(ParserProxyHandler.topic);

                    MessageParser.parseMessage(topicName,  -1L,
                            -1L,buffer,(message) -> {
                                messages.add(message);
                            });
                    for (int i=0;i <messages.size();i++){
                        this.info= this.info + new String(ByteBufUtil.getBytes((messages.get(i)).getData()),"UTF8");
                    }
                    break;
                case SUBSCRIBE:

                    MessageMetadata msgMetadata = Commands.parseMessageMetadata(buffer);
                    ParserProxyHandler.topic = cmd.getSubscribe().getTopic();
                    info = "{consumer:"+cmd.getSubscribe().getConsumerName()+",sequenceid:"+msgMetadata.getSequenceId()+",topic:"+cmd.getSubscribe().getTopic()+"}";
                    break;
                case MESSAGE:
                    //MessageMetadata msgMetadata = Commands.parseMessageMetadata(buffer);
                    topicName=TopicName.get(ParserProxyHandler.topic);
                    messages = Lists.newArrayList();
                    //topicName = TopicName.get("persistent://proxy-tenant/proxy-namespace/proxy-v0");
                    MessageParser.parseMessage(topicName,  -1L,
                            -1L,buffer,(message) -> {
                                messages.add(message);
                            });
                    for (int i=0;i <messages.size();i++){
                        this.info= this.info + new String(ByteBufUtil.getBytes((messages.get(i)).getData()),"UTF8");
                    }
                    break;

            }
            if (type=="proxyconn"){
                log.info("cr:{} pi:{} po:{} pr:{} cmd:{} info:{}",channel.remoteAddress(),channel.localAddress(),ctx.channel().remoteAddress(),ctx.channel().localAddress(),cmd.getType(),info);

            }
            else if (type=="backendconn"){
                log.info("cr:{} pi:{} po:{} pr:{} cmd:{} info:{}",ctx.channel().remoteAddress(),ctx.channel().localAddress(),channel.localAddress(),channel.remoteAddress(),cmd.getType(),info);
            }

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
