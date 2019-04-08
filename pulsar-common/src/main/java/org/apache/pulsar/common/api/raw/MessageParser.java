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
package org.apache.pulsar.common.api.raw;

import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static com.scurrilous.circe.params.CrcParameters.CRC32C;
import static org.apache.pulsar.common.api.Commands.hasChecksum;
import static org.apache.pulsar.common.api.Commands.readChecksum;

import com.scurrilous.circe.IncrementalIntHash;
import com.scurrilous.circe.checksum.Crc32cSse42Provider;
import com.scurrilous.circe.crc.Sse42Crc32C;
import com.scurrilous.circe.crc.StandardCrcProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.PulsarDecoder;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;

@UtilityClass
@Slf4j
public class MessageParser {
    public interface MessageProcessor {
        void process(RawMessage message);
    }

    final static IncrementalIntHash CRC32C_HASH;

    static {
        if (Sse42Crc32C.isSupported()) {
            CRC32C_HASH = new Crc32cSse42Provider().getIncrementalInt(CRC32C);
            //log.info("SSE4.2 CRC32C provider initialized");
        } else {
            CRC32C_HASH = new StandardCrcProvider().getIncrementalInt(CRC32C);
            //log.warn("Failed to load Circe JNI library. Falling back to Java based CRC32c provider");
        }
    }
    /**
     * Parse a raw Pulsar entry payload and extract all the individual message that may be included in the batch. The
     * provided {@link MessageProcessor} will be invoked for each individual message.
     */
    public static void parseMessage(TopicName topicName, long ledgerId, long entryId, ByteBuf headersAndPayload,
            MessageProcessor processor) throws IOException {
        MessageMetadata msgMetadata = null;
        ByteBuf payload = headersAndPayload;
        ByteBuf uncompressedPayload = null;
        ReferenceCountedObject<MessageMetadata> refCntMsgMetadata = null;

        try {
            if (!verifyChecksum(topicName, headersAndPayload, ledgerId, entryId)) {
                // discard message with checksum error
                return;
            }

            try {
                System.out.println("readerindex in message......"+payload.readerIndex());
                msgMetadata = Commands.parseMessageMetadata(payload);
            } catch (Throwable t) {
                log.warn("[{}] Failed to deserialize metadata for message {}:{} - Ignoring", topicName, ledgerId, entryId);
                return;
            }

            if (msgMetadata.getEncryptionKeysCount() > 0) {
                throw new IOException("Cannot parse encrypted message " + msgMetadata + " on topic " + topicName);
            }

            uncompressedPayload = uncompressPayloadIfNeeded(topicName, msgMetadata, headersAndPayload, ledgerId,
                    entryId);

            if (uncompressedPayload == null) {
                // Message was discarded on decompression error
                return;
            }

            final int numMessages = msgMetadata.getNumMessagesInBatch();
            refCntMsgMetadata = new ReferenceCountedObject<>(msgMetadata, (x) -> x.recycle());

            if (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch()) {
                processor.process(
                        RawMessageImpl.get(refCntMsgMetadata, null, uncompressedPayload.retain(), ledgerId, entryId, 0));
            } else {
                // handle batch message enqueuing; uncompressed payload has all messages in batch
                receiveIndividualMessagesFromBatch(refCntMsgMetadata, uncompressedPayload, ledgerId, entryId, processor);
            }


        } finally {
            ReferenceCountUtil.safeRelease(uncompressedPayload);
            ReferenceCountUtil.safeRelease(refCntMsgMetadata);
        }
    }

    public static boolean verifyChecksum(TopicName topic, ByteBuf headersAndPayload, long ledgerId, long entryId) throws UnsupportedEncodingException {

        if (hasChecksum(headersAndPayload)) {
            int checksum = readChecksum(headersAndPayload);

            if (headersAndPayload.hasMemoryAddress() && (CRC32C_HASH instanceof Sse42Crc32C)) {
                System.out.println("");
                System.out.println("1.......");
                System.out.println(headersAndPayload.memoryAddress()+" "+headersAndPayload.readerIndex()+" "+headersAndPayload.readableBytes());

                CRC32C_HASH.calculate(headersAndPayload.memoryAddress() + headersAndPayload.readerIndex(), headersAndPayload.readableBytes());
                System.out.println(CRC32C_HASH.calculate(headersAndPayload.memoryAddress() + headersAndPayload.readerIndex(), headersAndPayload.readableBytes()));
            } else if (headersAndPayload.hasArray()) {
                System.out.println("2.......");
                CRC32C_HASH.calculate(headersAndPayload.array(), headersAndPayload.arrayOffset() + headersAndPayload.readerIndex(),
                        headersAndPayload.readableBytes());
            } else {
                System.out.println("3.......");
                CRC32C_HASH.calculate(headersAndPayload.nioBuffer());
            }

            int computedChecksum = computeChecksum(headersAndPayload);
            System.out.println("verifyChecksum: "+new String(ByteBufUtil.getBytes(headersAndPayload),"UTF8")+"#"+checksum+"#"+computedChecksum);
            if (checksum != computedChecksum) {
                log.error(
                        "[{}] Checksum mismatch for message at {}:{}. Received checksum: 0x{}, Computed checksum: 0x{}",
                        topic, ledgerId, entryId, Long.toHexString(checksum), Integer.toHexString(computedChecksum));
                return false;
            }
        }

        return true;
    }

    public static ByteBuf uncompressPayloadIfNeeded(TopicName topic, MessageMetadata msgMetadata,
            ByteBuf payload, long ledgerId, long entryId) {
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(msgMetadata.getCompression());
        int uncompressedSize = msgMetadata.getUncompressedSize();
        int payloadSize = payload.readableBytes();
        if (payloadSize > PulsarDecoder.MaxMessageSize) {
            // payload size is itself corrupted since it cannot be bigger than the MaxMessageSize
            log.error("[{}] Got corrupted payload message size {} at {}:{}", topic, payloadSize,
                    ledgerId, entryId);
            return null;
        }

        try {
            ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);
            return uncompressedPayload;
        } catch (IOException e) {
            log.error("[{}] Failed to decompress message with {} at {}:{} : {}", topic,
                    msgMetadata.getCompression(), ledgerId, entryId, e.getMessage(), e);
            return null;
        }
    }

    private static void receiveIndividualMessagesFromBatch(ReferenceCountedObject<MessageMetadata> msgMetadata,
            ByteBuf uncompressedPayload, long ledgerId, long entryId, MessageProcessor processor) {
        int batchSize = msgMetadata.get().getNumMessagesInBatch();

        try {
            for (int i = 0; i < batchSize; ++i) {
                PulsarApi.SingleMessageMetadata.Builder singleMessageMetadataBuilder = PulsarApi.SingleMessageMetadata
                        .newBuilder();
                ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                        singleMessageMetadataBuilder, i, batchSize);

                if (singleMessageMetadataBuilder.getCompactedOut()) {
                    // message has been compacted out, so don't send to the user
                    singleMessagePayload.release();
                    singleMessageMetadataBuilder.recycle();
                    continue;
                }

                processor.process(RawMessageImpl.get(msgMetadata, singleMessageMetadataBuilder, singleMessagePayload,
                        ledgerId, entryId, i));
            }
        } catch (IOException e) {
            log.warn("Unable to obtain messages in batch", e);
        }
    }

}
