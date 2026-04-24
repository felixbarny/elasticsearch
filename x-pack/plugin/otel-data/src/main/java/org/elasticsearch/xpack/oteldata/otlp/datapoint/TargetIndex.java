/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.Set;

/**
 * Represents the target index for a data point, which can be either a specific index or a data stream.
 * The index is determined based on attributes, scope name, and default values.
 */
public final class TargetIndex {

    public static final String TYPE_METRICS = "metrics";

    private static final String RECEIVER = "/receiver/";
    private static final String CONNECTOR = "/connector/";
    private static final Set<String> SELF_TELEMETRY_SCOPES = Set.of(
        "go.opentelemetry.io/collector/receiver/receiverhelper",
        "go.opentelemetry.io/collector/scraper/scraperhelper",
        "go.opentelemetry.io/collector/processor/processorhelper",
        "go.opentelemetry.io/collector/exporter/exporterhelper",
        "go.opentelemetry.io/collector/service"
    );
    private static final String SELF_TELEMETRY_DATASET = "collectortelemetry";
    private static final String ENCODING_FORMAT = "encoding.format";
    private static final String ELASTICSEARCH_INDEX = "elasticsearch.index";
    private static final String DATA_STREAM_DATASET = "data_stream.dataset";
    private static final String DATA_STREAM_NAMESPACE = "data_stream.namespace";
    private static final String DEFAULT_DATASET = "generic";
    private static final String OTEL_DATASET_SUFFIX = ".otel";
    private static final String DEFAULT_NAMESPACE = "default";
    private static final TargetIndex DEFAULT_METRICS_TARGET = evaluate(TYPE_METRICS, List.of(), null, List.of(), List.of());

    private String index;
    private String type;
    private String dataset;
    private String namespace;

    public static TargetIndex defaultMetrics() {
        return DEFAULT_METRICS_TARGET;
    }

    public static TargetIndex evaluate(String type, List<KeyValue> attributes, InstrumentationScope scope, List<KeyValue> resourceAttributes) {
        return evaluate(type, attributes, extractScopeRoutingDataset(scope), scope.getAttributesList(), resourceAttributes);
    }

    public static boolean isTargetIndexAttribute(String attributeKey) {
        return attributeKey.equals(ELASTICSEARCH_INDEX)
            || attributeKey.equals(DATA_STREAM_DATASET)
            || attributeKey.equals(DATA_STREAM_NAMESPACE);
    }

    /**
     * Determines the target index for a data point.
     *
     * @param type The data stream type (e.g., "metrics", "logs").
     * @param attributes The attributes associated with the data point.
     * @param receiverName The name of the receiver, which may influence the dataset (receiver-based routing).
     * @param scopeAttributes Attributes associated with the scope.
     * @param resourceAttributes Attributes associated with the resource.
     * @return A TargetIndex instance representing the target index for the data point.
     */
    public static TargetIndex evaluate(
        String type,
        List<KeyValue> attributes,
        @Nullable String receiverName,
        List<KeyValue> scopeAttributes,
        List<KeyValue> resourceAttributes
    ) {
        // Order:
        // 1. elasticsearch.index from attributes, scope.attributes, resource.attributes
        // 2. read data_stream.* from attributes, scope.attributes, resource.attributes
        // 3. receiver-based routing based on scope.name
        // 4. use default hardcoded data_stream.* (<type>-generic-default)
        TargetIndex target = new TargetIndex();
        target.populateFrom(attributes);
        target.populateFrom(scopeAttributes);
        target.populateFrom(resourceAttributes);
        if (target.index == null) {
            target.type = type;
            if (target.dataset == null && receiverName != null) {
                target.dataset = receiverName;
            }
            target.dataset = DataStream.sanitizeDataset(target.dataset);
            if (target.dataset == null) {
                target.dataset = DEFAULT_DATASET;
            }
            // add otel suffix to match OTel index template
            target.dataset = target.dataset + OTEL_DATASET_SUFFIX;
            target.namespace = DataStream.sanitizeNamespace(target.namespace);

            if (target.namespace == null) {
                target.namespace = DEFAULT_NAMESPACE;
            }
            target.index = target.type + "-" + target.dataset + "-" + target.namespace;
        } else {
            target.type = null;
            target.dataset = null;
            target.namespace = null;
        }
        return target;
    }

    public static @Nullable String extractReceiverName(InstrumentationScope scope) {
        String scopeName = scope.getName();
        int indexOfReceiver = scopeName.indexOf(RECEIVER);
        if (indexOfReceiver >= 0) {
            int beginIndex = indexOfReceiver + RECEIVER.length();
            int endIndex = scopeName.indexOf('/', beginIndex);
            if (endIndex < 0) {
                endIndex = scopeName.length();
            }
            return scopeName.substring(beginIndex, endIndex);
        }
        return null;
    }

    private static @Nullable String extractScopeRoutingDataset(InstrumentationScope scope) {
        String scopeName = scope.getName();
        if (SELF_TELEMETRY_SCOPES.contains(scopeName)) {
            return SELF_TELEMETRY_DATASET;
        }
        for (int i = 0, size = scope.getAttributesCount(); i < size; i++) {
            KeyValue attribute = scope.getAttributes(i);
            if (ENCODING_FORMAT.equals(attribute.getKey()) && attribute.getValue().hasStringValue()) {
                String format = attribute.getValue().getStringValue();
                if (format.isEmpty() == false) {
                    return format;
                }
                break;
            }
        }
        String receiver = extractComponentName(scopeName, RECEIVER);
        if (receiver != null) {
            return receiver;
        }
        return extractComponentName(scopeName, CONNECTOR);
    }

    private static @Nullable String extractComponentName(String scopeName, String marker) {
        int indexOfMarker = scopeName.indexOf(marker);
        if (indexOfMarker < 0) {
            return null;
        }
        int beginIndex = indexOfMarker + marker.length();
        int endIndex = scopeName.indexOf('/', beginIndex);
        if (endIndex < 0) {
            endIndex = scopeName.length();
        }
        if (beginIndex >= endIndex) {
            return null;
        }
        return scopeName.substring(beginIndex, endIndex);
    }

    private TargetIndex() {}

    private void populateFrom(List<KeyValue> attributes) {
        if (isPopulated()) {
            return;
        }
        for (int i = 0, size = attributes.size(); i < size; i++) {
            KeyValue attr = attributes.get(i);
            if (attr.getKey().equals(ELASTICSEARCH_INDEX)) {
                index = attr.getValue().getStringValue();
            } else if (dataset == null && attr.getKey().equals(DATA_STREAM_DATASET)) {
                dataset = attr.getValue().getStringValue();
            } else if (namespace == null && attr.getKey().equals(DATA_STREAM_NAMESPACE)) {
                namespace = attr.getValue().getStringValue();
            }
        }
    }

    private boolean isPopulated() {
        return (dataset != null && namespace != null) || index != null;
    }

    public boolean isDataStream() {
        return type != null && dataset != null && namespace != null;
    }

    public String index() {
        return index;
    }

    public String type() {
        return type;
    }

    public String dataset() {
        return dataset;
    }

    public String namespace() {
        return namespace;
    }

    @Override
    public String toString() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        TargetIndex that = (TargetIndex) o;
        return index.equals(that.index);
    }

    @Override
    public int hashCode() {
        return index.hashCode();
    }
}
