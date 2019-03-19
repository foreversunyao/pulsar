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
import io.netty.channel.*;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.pulsar.common.api.PulsarDecoder;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ParserProxyHandler extends PulsarDecoder {

    private Channel frontEndChannel;
    private Channel backEndChannel;

    private Long costProxy;
    private LengthFieldBasedFrameDecoder msgDecoder=  new LengthFieldBasedFrameDecoder(PulsarDecoder.MaxFrameSize, 0, 4, 0, 4);


    @Override
    protected void messageReceived() {
        // no-op
    }

    public ParserProxyHandler() {
    }

    public void parseConn(Channel frontEndChannel,Channel backEndChannel,Long costProxy, Object msg){
        this.frontEndChannel = frontEndChannel;
        this.backEndChannel = backEndChannel;
        this.costProxy = costProxy;
        try {
            this.parseMsg(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void parseMsg(Object msg)  {
        ByteBuf buffer = (ByteBuf) msg;
        PulsarApi.BaseCommand cmd = null;
        PulsarApi.BaseCommand.Builder cmdBuilder = null;
        for (int i=0;i<((ByteBuf) msg).capacity();i++){
           //System.out.print("#");
           System.out.print(buffer.getByte(i));
        }
        //msgDecoder.channelRead(null,msg);
        //super.channelRead(null,msg);
        System.out.println();
        log.info("{}#{}#{}#{}#{}#{}",frontEndChannel.remoteAddress(),frontEndChannel.localAddress(),backEndChannel.localAddress(),backEndChannel.remoteAddress(),costProxy,buffer.toString());
    }


    private static final Logger log = LoggerFactory.getLogger(ParserProxyHandler.class);
}
