/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.proto;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;

import com.google.protobuf.WireFormat;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.oteldata.otlp.OtlpUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IncrementalProtobufParserTests extends ESTestCase {

    private static final int FIELD_RESOURCE_METRICS = 1;
    private static final int PROTOBUF_TAG_TYPE_BITS = 3;

    public void testParsesWhenLengthVarintSpansMultipleChunks() throws Exception {
        ExportMetricsServiceRequest request = createLargeRequest();
        byte[] payload = request.toByteArray();

        assertEquals((FIELD_RESOURCE_METRICS << PROTOBUF_TAG_TYPE_BITS) | WireFormat.WIRETYPE_LENGTH_DELIMITED, payload[0] & 0xFF);
        assertTrue(countVarintBytes(payload, 1) > 1);

        List<ResourceMetrics> parsed = new ArrayList<>();
        IncrementalProtobufParser parser = createResourceMetricsParser();

        BytesArray firstChunk = new BytesArray(Arrays.copyOfRange(payload, 0, 2));
        assertEquals(0, parser.parse(firstChunk, false, frame -> { parsed.add(ResourceMetrics.parseFrom(frame.streamInput())); }));
        assertEquals(List.of(), parsed);

        BytesArray secondChunk = new BytesArray(Arrays.copyOfRange(payload, 2, payload.length));
        var data = CompositeBytesReference.of(firstChunk, secondChunk);
        int consumed = parser.parse(data, true, frame -> parsed.add(ResourceMetrics.parseFrom(frame.streamInput())));
        assertEquals(payload.length, consumed);
        assertEquals(List.of(request.getResourceMetrics(0)), parsed);
    }

    public void testParsesWhenResourceMetricsPayloadSpansChunks() throws Exception {
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(List.of(createMetric()));
        byte[] payload = request.toByteArray();

        List<ResourceMetrics> parsed = new ArrayList<>();
        IncrementalProtobufParser parser = createResourceMetricsParser();

        BytesArray firstChunk = new BytesArray(Arrays.copyOfRange(payload, 0, payload.length - 1));
        assertEquals(0, parser.parse(firstChunk, false, frame -> { parsed.add(ResourceMetrics.parseFrom(frame.streamInput())); }));
        assertEquals(List.of(), parsed);

        BytesArray secondChunk = new BytesArray(Arrays.copyOfRange(payload, payload.length - 1, payload.length));
        var data = CompositeBytesReference.of(firstChunk, secondChunk);
        int consumed = parser.parse(data, true, frame -> parsed.add(ResourceMetrics.parseFrom(frame.streamInput())));
        assertEquals(payload.length, consumed);
        assertEquals(List.of(request.getResourceMetrics(0)), parsed);
    }

    public void testSkipsUnknownTopLevelFields() throws Exception {
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(List.of(createMetric()));
        byte[] payload = request.toByteArray();

        int unknownVarintFieldTag = (20 << PROTOBUF_TAG_TYPE_BITS) | WireFormat.WIRETYPE_VARINT;
        byte[] prefix = encodeVarint(unknownVarintFieldTag, 1);

        List<ResourceMetrics> parsed = new ArrayList<>();
        IncrementalProtobufParser parser = createResourceMetricsParser();
        var data = CompositeBytesReference.of(new BytesArray(prefix), new BytesArray(payload));
        int consumed = parser.parse(data, true, frame -> parsed.add(ResourceMetrics.parseFrom(frame.streamInput())));

        assertEquals(prefix.length + payload.length, consumed);
        assertEquals(List.of(request.getResourceMetrics(0)), parsed);
    }

    public void testFailsOnInvalidTagFieldNumberZeroWhenLastChunk() {
        int invalidTag = (0 << PROTOBUF_TAG_TYPE_BITS) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
        byte[] payload = encodeVarint(invalidTag, 0);

        IncrementalProtobufParser parser = createResourceMetricsParser();
        ElasticsearchParseException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> parser.parse(new BytesArray(payload), true, frame -> {})
        );
        assertTrue(exception.getMessage().contains("invalid protobuf field number [0]"));
    }

    public void testFailsOnInvalidTagFieldNumberZeroWhenMoreChunksExpected() {
        int invalidTag = (0 << PROTOBUF_TAG_TYPE_BITS) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
        byte[] payload = encodeVarint(invalidTag, 0);

        IncrementalProtobufParser parser = createResourceMetricsParser();
        ElasticsearchParseException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> parser.parse(new BytesArray(payload), false, frame -> {})
        );
        assertTrue(exception.getMessage().contains("invalid protobuf field number [0]"));
    }

    public void testFailsOnIncompleteLastChunk() {
        ExportMetricsServiceRequest request = OtlpUtils.createMetricsRequest(List.of(createMetric()));
        byte[] payload = Arrays.copyOf(request.toByteArray(), request.toByteArray().length - 1);

        IncrementalProtobufParser parser = createResourceMetricsParser();
        ElasticsearchParseException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> parser.parse(new BytesArray(payload), true, frame -> {})
        );
        assertTrue(exception.getMessage().contains("incomplete protobuf payload"));
    }

    private static IncrementalProtobufParser createResourceMetricsParser() {
        return new IncrementalProtobufParser(FIELD_RESOURCE_METRICS);
    }

    private static Metric createMetric() {
        return OtlpUtils.createGaugeMetric("metric", "", List.of(OtlpUtils.createDoubleDataPoint(0)));
    }

    private static ExportMetricsServiceRequest createLargeRequest() {
        List<io.opentelemetry.proto.common.v1.KeyValue> attributes = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            attributes.add(OtlpUtils.keyValue("k" + i, "value-" + i + "-abcdefghijklmnopqrstuvwxyz"));
        }
        Metric metric = OtlpUtils.createGaugeMetric("large.metric", "", List.of(OtlpUtils.createDoubleDataPoint(0, 0, attributes)));
        return OtlpUtils.createMetricsRequest(List.of(metric));
    }

    private static int countVarintBytes(byte[] bytes, int offset) {
        int count = 0;
        while (offset + count < bytes.length) {
            if ((bytes[offset + count] & 0x80) == 0) {
                return count + 1;
            }
            count++;
        }
        fail("unterminated varint");
        return -1;
    }

    private static byte[] encodeVarint(int... values) {
        List<Byte> encoded = new ArrayList<>();
        for (int value : values) {
            long current = value & 0xFFFFFFFFL;
            while ((current & ~0x7FL) != 0) {
                encoded.add((byte) ((current & 0x7F) | 0x80));
                current >>>= 7;
            }
            encoded.add((byte) current);
        }
        byte[] bytes = new byte[encoded.size()];
        for (int i = 0; i < encoded.size(); i++) {
            bytes[i] = encoded.get(i);
        }
        return bytes;
    }
}
