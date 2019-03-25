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

import static org.apache.commons.lang3.StringUtils.isEmpty;

import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.common.api.PulsarDecoder;
import org.apache.pulsar.common.util.ClientSslContextRefresher;
import org.apache.pulsar.common.util.NettySslContextBuilder;
import org.apache.pulsar.common.util.SslContextAutoRefreshBuilder;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;

/**
 * Initialize service channel handlers.
 *
 */
public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";
    private final ProxyService proxyService;
    private final NettySslContextBuilder serverSslCtxRefresher;
    private final ClientSslContextRefresher clientSslCtxRefresher;
    private final boolean enableTls;

    public ServiceChannelInitializer(ProxyService proxyService, ProxyConfiguration serviceConfig, boolean enableTls)
            throws Exception {
        super();
        this.proxyService = proxyService;
        this.enableTls = enableTls;

        if (enableTls) {
            serverSslCtxRefresher = new NettySslContextBuilder(serviceConfig.isTlsAllowInsecureConnection(),
                    serviceConfig.getTlsTrustCertsFilePath(), serviceConfig.getTlsCertificateFilePath(),
                    serviceConfig.getTlsKeyFilePath(), serviceConfig.getTlsCiphers(), serviceConfig.getTlsProtocols(),
                    serviceConfig.isTlsRequireTrustedClientCertOnConnect(),
                    serviceConfig.getTlsCertRefreshCheckDurationSec());
        } else {
            this.serverSslCtxRefresher = null;
        }

        if (serviceConfig.isTlsEnabledWithBroker()) {
            AuthenticationDataProvider authData = null;

            if (!isEmpty(serviceConfig.getBrokerClientAuthenticationPlugin())) {
                authData = AuthenticationFactory.create(serviceConfig.getBrokerClientAuthenticationPlugin(),
                        serviceConfig.getBrokerClientAuthenticationParameters()).getAuthData();
            }

            clientSslCtxRefresher = new ClientSslContextRefresher(serviceConfig.isTlsAllowInsecureConnection(),
                    serviceConfig.getBrokerClientTrustCertsFilePath(), authData);

        } else {
            this.clientSslCtxRefresher = null;
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if (serverSslCtxRefresher != null && this.enableTls) {
            SslContext sslContext = serverSslCtxRefresher.get();
            if (sslContext != null) {
                ch.pipeline().addLast(TLS_HANDLER, sslContext.newHandler(ch.alloc()));
            }
        }
        System.out.println("1, Proxy init inbound channel:"+ch.localAddress()+"#"+ch.remoteAddress());
        ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(PulsarDecoder.MaxFrameSize, 0, 4, 0, 4));
        ch.pipeline().addLast("parser", new ParserProxyHandler());
        ch.pipeline().addLast("handler",
                new ProxyConnection(proxyService, clientSslCtxRefresher == null ? null : clientSslCtxRefresher.get()));
    }
}
