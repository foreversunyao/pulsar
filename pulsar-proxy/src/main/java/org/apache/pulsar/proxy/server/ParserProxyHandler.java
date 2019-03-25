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
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.raw.MessageParser;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.channel.Channel;
import java.util.List;
import java.util.Hashtable;
import java.util.Map;


public class ParserProxyHandler extends ChannelInboundHandlerAdapter {


    private Channel channel;
    private static final int lengthFieldLength = 4;
    private String topic="";
    private String type;
    private List<RawMessage> messages = null;
    private static Map<String, String> producerHashTable = new Hashtable<>();
    private static Map<String, String> consumerHashTable = new Hashtable<>();

    public ParserProxyHandler(Channel channel, String type){
        this.channel = channel;
        this.type=type;
    }

    private void logging (PulsarApi.BaseCommand cmd, String info){


    }
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        PulsarApi.BaseCommand cmd = null;
        PulsarApi.BaseCommand.Builder cmdBuilder = null;

        TopicName topicName = null;
        String info="";
        String conn ="";
        System.out.println("######channelid:"+ctx.channel().id());
        ByteBuf buffer = (ByteBuf)(msg);

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
                    info = " {producer:"+cmd.getProducer().getProducerId()+",topic:"+cmd.getProducer().getTopic()+"}";
                    this.topic=cmd.getProducer().getTopic();
                    ParserProxyHandler.producerHashTable.put(String.valueOf(cmd.getProducer().getProducerId()),cmd.getProducer().getTopic());

                    break;
                case SEND:
                    System.out.println("#######producerid:"+cmd.getProducer().getProducerId());
                    messages = Lists.newArrayList();
                    topicName = TopicName.get(ParserProxyHandler.producerHashTable.get(String.valueOf(cmd.getProducer().getProducerId())));

                    MessageParser.parseMessage(topicName,  -1L,
                            -1L,buffer,(message) -> {
                                messages.add(message);
                            });
                    for (int i=0;i <messages.size();i++){
                        info= info + new String(ByteBufUtil.getBytes((messages.get(i)).getData()),"UTF8");
                    }
                    break;
                case SUBSCRIBE:
                    this.topic = cmd.getSubscribe().getTopic();
                    info = "{consumer:"+cmd.getSubscribe().getConsumerId()+",topic:"+cmd.getSubscribe().getTopic()+"}";
                    ParserProxyHandler.consumerHashTable.put(String.valueOf(cmd.getSubscribe().getConsumerId()),cmd.getSubscribe().getTopic());
                    break;
                case MESSAGE:
                    System.out.println("#######consumerid:"+cmd.getMessage().getConsumerId());
                    //MessageMetadata msgMetadata = Commands.parseMessageMetadata(buffer);

                    //topicName=TopicName.get(this.topic);
                    topicName = TopicName.get(ParserProxyHandler.consumerHashTable.get(String.valueOf(cmd.getMessage().getConsumerId())));
                    messages = Lists.newArrayList();
                    //topicName = TopicName.get("persistent://proxy-tenant/proxy-namespace/proxy-v0");
                    MessageParser.parseMessage(topicName,  -1L,
                            -1L,buffer,(message) -> {
                                messages.add(message);
                            });
                    for (int i=0;i <messages.size();i++){
                        info= info + new String(ByteBufUtil.getBytes((messages.get(i)).getData()),"UTF8");
                    }
                    break;

            }
            if (this.type=="proxyconn"){
                conn = "["+ ctx.channel().remoteAddress()+"|"+ctx.channel().localAddress()+"|"+channel.localAddress()+"|"+channel.remoteAddress()+"]";
            }
            else if (this.type=="backendconn"){
                conn = "["+channel.remoteAddress()+"|"+channel.localAddress()+"|"+ctx.channel().localAddress()+"|"+ctx.channel().remoteAddress()+"]";
            }
            log.info("conn:{} cmd:{} msg:{}",conn,cmd.getType(),info);

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
        ctx.fireChannelRead(msg);
    }

    private static final Logger log = LoggerFactory.getLogger(ParserProxyHandler.class);
}
