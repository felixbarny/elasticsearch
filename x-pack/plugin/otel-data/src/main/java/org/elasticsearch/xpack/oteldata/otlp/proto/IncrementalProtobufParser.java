/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.proto;

import com.google.protobuf.WireFormat;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;

/**
 * Incrementally parses {@code repeated} top-level protobuf fields and emits fully available length-delimited
 * payload bytes (frames) for one configured field number.
 */
public final class IncrementalProtobufParser {

    private static final int MAX_VARINT_BYTES = 10;
    private static final int INCOMPLETE_VARINT = 0;

    private final int targetLengthDelimitedFieldNumber;
    private long parsedVarintValue;

    /**
     * Receives a fully available protobuf frame as a {@link BytesReference} slice. The reference is
     * only valid for the duration of the call; the caller retains ownership and may release it afterwards.
     */
    @FunctionalInterface
    public interface FrameConsumer {
        void accept(BytesReference frame) throws IOException;
    }

    /**
     * Creates a parser that scans top-level protobuf fields and emits payload bytes for one configured
     * length-delimited field number.
     *
     * @param targetLengthDelimitedFieldNumber field number to emit payloads for
     */
    public IncrementalProtobufParser(int targetLengthDelimitedFieldNumber) {
        this.targetLengthDelimitedFieldNumber = targetLengthDelimitedFieldNumber;
    }

    /**
     * Parses as many complete protobuf fields as possible from {@code data}.
     *
     * @param data available unparsed bytes
     * @param isLast whether no further bytes will arrive for this request
     * @param frameConsumer callback invoked with each complete frame as a {@link BytesReference} slice of {@code data}
     * @return number of bytes consumed from {@code data}
     */
    public int parse(BytesReference data, boolean isLast, FrameConsumer frameConsumer) throws IOException {
        int offset = 0;
        final int dataLength = data.length();
        while (offset < dataLength) {
            final int fieldStart = offset;

            int tagBytesConsumed = tryParseVarint(data, offset, dataLength);
            if (tagBytesConsumed == INCOMPLETE_VARINT) {
                return handleIncompleteField(isLast, fieldStart);
            }
            offset += tagBytesConsumed;

            long tag = parsedVarintValue;
            if (tag <= 0 || tag > Integer.MAX_VALUE) {
                throw new ElasticsearchParseException("invalid protobuf field tag [" + tag + "]");
            }

            int decodedTag = (int) tag;
            int fieldNumber = WireFormat.getTagFieldNumber(decodedTag);
            int wireType = WireFormat.getTagWireType(decodedTag);
            if (fieldNumber <= 0) {
                throw new ElasticsearchParseException("invalid protobuf field number [" + fieldNumber + "]");
            }
            // We only consume one configured length-delimited field, but must still skip other wire types
            // so unknown top-level fields do not break framing of later target fields.
            switch (wireType) {
                case WireFormat.WIRETYPE_VARINT -> {
                    int valueBytesConsumed = tryParseVarint(data, offset, dataLength);
                    if (valueBytesConsumed == INCOMPLETE_VARINT) {
                        return handleIncompleteField(isLast, fieldStart);
                    }
                    offset += valueBytesConsumed;
                }
                case WireFormat.WIRETYPE_FIXED64 -> {
                    if (dataLength - offset < Long.BYTES) {
                        return handleIncompleteField(isLast, fieldStart);
                    }
                    offset += Long.BYTES;
                }
                case WireFormat.WIRETYPE_LENGTH_DELIMITED -> {
                    int lengthBytesConsumed = tryParseVarint(data, offset, dataLength);
                    if (lengthBytesConsumed == INCOMPLETE_VARINT) {
                        return handleIncompleteField(isLast, fieldStart);
                    }
                    offset += lengthBytesConsumed;
                    if (parsedVarintValue < 0 || parsedVarintValue > Integer.MAX_VALUE) {
                        throw new ElasticsearchParseException("invalid protobuf length [" + parsedVarintValue + "]");
                    }
                    int fieldLength = (int) parsedVarintValue;
                    if (dataLength - offset < fieldLength) {
                        return handleIncompleteField(isLast, fieldStart);
                    }
                    if (fieldNumber == targetLengthDelimitedFieldNumber) {
                        frameConsumer.accept(data.slice(offset, fieldLength));
                    }
                    offset += fieldLength;
                }
                case WireFormat.WIRETYPE_FIXED32 -> {
                    if (dataLength - offset < Integer.BYTES) {
                        return handleIncompleteField(isLast, fieldStart);
                    }
                    offset += Integer.BYTES;
                }
                default -> throw new ElasticsearchParseException("unsupported protobuf wire type [" + wireType + "]");
            }
        }
        return offset;
    }

    private int handleIncompleteField(boolean isLast, int fieldStart) {
        if (isLast) {
            throw new ElasticsearchParseException("incomplete protobuf payload");
        }
        return fieldStart;
    }

    private int tryParseVarint(BytesReference data, int offset, int dataLength) {
        if (offset >= dataLength) {
            return INCOMPLETE_VARINT;
        }

        int firstByte = data.get(offset);
        if (firstByte >= 0) {
            parsedVarintValue = firstByte;
            return 1;
        }

        long value = firstByte & 0x7FL;
        int shift = 7;
        for (int i = 1; i < MAX_VARINT_BYTES; i++) {
            int currentOffset = offset + i;
            if (currentOffset >= dataLength) {
                return INCOMPLETE_VARINT;
            }
            int currentByte = data.get(currentOffset);
            value |= (long) (currentByte & 0x7F) << shift;
            if (currentByte >= 0) {
                parsedVarintValue = value;
                return i + 1;
            }
            shift += 7;
        }
        throw new ElasticsearchParseException("invalid varint in protobuf payload");
    }
}
