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
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.pulsar.common.api.PulsarDecoder;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.raw.MessageParser;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.channel.Channel;
import io.netty.buffer.Unpooled;
import io.netty.buffer.CompositeByteBuf;
import org.apache.pulsar.common.api.proto.PulsarApi;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;


public class ParserProxyHandler extends ChannelInboundHandlerAdapter {


    private Channel channel;
    private static final int lengthFieldLength = 4;
    public static final String frontendConn = "frontendconn";
    public static final String backendConn = "backendconn";

    private String connType;
    //LengthFieldBasedFrameDecoder lengthFieldBasedFrameDecoder = new LengthFieldBasedFrameDecoder(PulsarDecoder.MaxFrameSize, 0, 4, 0, 4);



    //producerid/consumerid+channelid as key
    private static Map<String, String> producerHashMap = new ConcurrentHashMap<>();
    private static Map<String, String> consumerHashMap = new ConcurrentHashMap<>();

    public ParserProxyHandler(Channel channel, String type){
        this.channel = channel;
        this.connType=type;
    }

    private void logging (Channel conn,PulsarApi.BaseCommand.Type cmdtype,String info,List<RawMessage> messages) throws Exception{
        String connString="";

        if (messages !=null){
            Long timeCost;
            for (int i=0;i <messages.size();i++){
                timeCost = System.currentTimeMillis() - messages.get(i).getPublishTime();
                info = info + "["+timeCost+"] "+new String(ByteBufUtil.getBytes((messages.get(i)).getData()),"UTF8");
            }
        }

        switch (this.connType){
            case ParserProxyHandler.frontendConn:
                connString = "["+ conn.remoteAddress().toString()+""+conn.localAddress()+channel.localAddress()+channel.remoteAddress()+"]";
                break;
            case ParserProxyHandler.backendConn:
                connString  = "["+channel.remoteAddress()+channel.localAddress()+conn.localAddress()+conn.remoteAddress()+"]";
                break;
        }
        log.info("conn:{} cmd:{} msg:{}",connString,cmdtype,info);


    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        PulsarApi.BaseCommand cmd = null;
        PulsarApi.BaseCommand.Builder cmdBuilder = null;
        TopicName topicName = null;
        List<RawMessage> messages = Lists.newArrayList();
        ByteBuf buffer = (ByteBuf)(msg);

        System.out.println("#####reader_index:"+buffer.readerIndex()+" writer_index:"+buffer.writerIndex());

        //MessageMetadata msgMetadata = null;
        try {
            System.out.println("#0:"+ProxyService.proxylogLevel+" "+buffer.readableBytes());

            for (int i =0; i< buffer.readableBytes();i++){
                System.out.print(String.format("%02X ", buffer.getByte(i)));
            }

            //
            buffer.markReaderIndex();
            buffer.markWriterIndex();

            //skip lengthFieldLength
            // buffer.readerIndex(ParserProxyHandler.lengthFieldLength);


            int cmdSize = (int) buffer.readUnsignedInt();
            int writerIndex = buffer.writerIndex();
            buffer.writerIndex(buffer.readerIndex() + cmdSize);
            ByteBufCodedInputStream cmdInputStream = ByteBufCodedInputStream.get(buffer);

            cmdBuilder = PulsarApi.BaseCommand.newBuilder();
            cmd = cmdBuilder.mergeFrom(cmdInputStream, null).build();
            buffer.writerIndex(writerIndex);
            cmdInputStream.recycle();
            System.out.println(cmd.getType());
            switch (cmd.getType()) {
                case PRODUCER:
                    ParserProxyHandler.producerHashMap.put(String.valueOf(cmd.getProducer().getProducerId())+","+String.valueOf(ctx.channel().id()),cmd.getProducer().getTopic());

                    logging(ctx.channel(),cmd.getType(),"{producer:"+cmd.getProducer().getProducerName()+",topic:"+cmd.getProducer().getTopic()+"}",null);
                    break;

                case SEND:
                    if (ProxyService.proxylogLevel !=2){
                        logging(ctx.channel(),cmd.getType(),"",null);
                        break;
                    }
                    topicName = TopicName.get(ParserProxyHandler.producerHashMap.get(String.valueOf(cmd.getProducer().getProducerId())+","+String.valueOf(ctx.channel().id())));
                    MessageParser.parseMessage(topicName,  -1L,
                            -1L,buffer,(message) -> {
                                messages.add(message);
                            });

                    logging(ctx.channel(),cmd.getType(),"",messages);
                    break;

                case SUBSCRIBE:
                    ParserProxyHandler.consumerHashMap.put(String.valueOf(cmd.getSubscribe().getConsumerId())+","+String.valueOf(ctx.channel().id()),cmd.getSubscribe().getTopic());

                    logging(ctx.channel(),cmd.getType(),"{consumer:"+cmd.getSubscribe().getConsumerName()+",topic:"+cmd.getSubscribe().getTopic()+"}",null);
                    break;

                case MESSAGE:
                    System.out.println("#1:"+ProxyService.proxylogLevel);
                    if (ProxyService.proxylogLevel !=2){
                        logging(ctx.channel(),cmd.getType(),"",null);
                        break;
                    }
                    System.out.println("#2:"+ProxyService.proxylogLevel);
                    topicName = TopicName.get(ParserProxyHandler.consumerHashMap.get(String.valueOf(cmd.getMessage().getConsumerId())+","+DirectProxyHandler.inboundOutboundChannelMap.get(ctx.channel().id())));
                    int msgTotalSize;
                    int msgCmdSize;
                    ByteBuf bufferSubMsg;

                    buffer.resetReaderIndex(); //set ReaderIndex to 0

                    while(buffer.readableBytes()>0){

                        msgTotalSize = buffer.readInt();
                        bufferSubMsg = buffer.copy(buffer.readerIndex()-4,4+msgTotalSize); //getSubMessageBuffer
                        bufferSubMsg.skipBytes(4); //skip msgTotalSize buffer
                        msgCmdSize=bufferSubMsg.readInt();
                        bufferSubMsg.skipBytes(msgCmdSize);

                        MessageParser.parseMessage(topicName,  -1L,
                                -1L,bufferSubMsg,(message) -> {
                                    messages.add(message);
                                });
                        buffer.skipBytes(msgTotalSize); //skip to next Message if exists
                    }
                    logging(ctx.channel(),cmd.getType(),"",messages);
                    break;

                    default:
                    logging(ctx.channel(),cmd.getType(),"",null);
                    break;
            }

        } catch (Exception e){
            log.error("{},{},{}",e.getMessage(),e.getStackTrace(),e.getCause());
        }finally {
            if (cmdBuilder != null) {
                cmdBuilder.recycle();
            }
            if (cmd != null) {
                cmd.recycle();
            }
            buffer.resetReaderIndex();
            buffer.resetWriterIndex();

            System.out.println();

            for (int i=0;i <buffer.readableBytes();i++){
                System.out.print(String.format("%02X ", buffer.getByte(i)));
            }
            System.out.println();
            System.out.println("1==============="+buffer.readableBytes());
            ByteBuf totalSizeBuf = Unpooled.buffer(4);

            totalSizeBuf.writeInt(buffer.readableBytes());
            System.out.println();
            CompositeByteBuf compBuf = Unpooled.compositeBuffer();
            compBuf.addComponents(totalSizeBuf,buffer);
            compBuf.writerIndex(4+buffer.readableBytes());
            System.out.println("#######concat#### "+compBuf.readableBytes()+" "+compBuf.readerIndex()+" "+compBuf.writerIndex());
            for (int i=0;i <compBuf.readableBytes();i++){
                System.out.print(String.format("%02X ", compBuf.getByte(i)));
            }
            System.out.println();

            ctx.fireChannelRead(compBuf);





        }
        //ctx.fireChannelRead(msg);
    }

    private static final Logger log = LoggerFactory.getLogger(ParserProxyHandler.class);
}
