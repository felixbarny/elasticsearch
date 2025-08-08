/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.common.v1.KeyValue;

import org.elasticsearch.cluster.metadata.DataStream;

import java.util.List;

final class IndexRouting {

    static final String ELASTICSEARCH_INDEX = "elasticsearch.index";
    static final String DATA_STREAM_DATASET = "data_stream.dataset";
    static final String DATA_STREAM_NAMESPACE = "data_stream.namespace";

    private String index;
    private String type;
    private String dataset;
    private String namespace;

    public static IndexRouting route(
        String type,
        List<KeyValue> attributes,
        String scopeName,
        List<KeyValue> scopeAttributes,
        List<KeyValue> resourceAttributes
    ) {
        // Order:
        // 1. elasticsearch.index from attributes, scope.attributes, resource.attributes
        // 2. read data_stream.* from attributes, scope.attributes, resource.attributes
        // 3. receiver-based routing based on scope.name
        // 4. use default hardcoded data_stream.* (<type>-generic-default)
        IndexRouting indexRouting = new IndexRouting();
        indexRouting.populateFrom(attributes);
        indexRouting.populateFrom(scopeAttributes);
        indexRouting.populateFrom(resourceAttributes);
        if (indexRouting.index == null) {
            indexRouting.type = type;
            indexRouting.dataset = DataStream.sanitizeDataset(indexRouting.dataset);
            if (indexRouting.dataset == null && scopeName != null) {
                int indexOfReceiver = scopeName.indexOf("/receiver/");
                if (indexOfReceiver >= 0) {
                    int beginIndex = indexOfReceiver + 10;
                    indexRouting.dataset = scopeName.substring(beginIndex, scopeName.indexOf('/', beginIndex));
                }
            }
            if (indexRouting.dataset == null) {
                indexRouting.dataset = "generic";
            }
            // add otel suffix to match OTel index template
            indexRouting.dataset = indexRouting.dataset + ".otel";
            indexRouting.namespace = DataStream.sanitizeNamespace(indexRouting.namespace);

            if (indexRouting.namespace == null) {
                indexRouting.namespace = "default";
            }
            indexRouting.index = indexRouting.type + "-" + indexRouting.dataset + "-" + indexRouting.namespace;
        }
        return indexRouting;
    }

    private void populateFrom(List<KeyValue> attributes) {
        if (isPopulated()) {
            return;
        }
        for (int i = 0, size = attributes.size(); i < size; i++) {
            KeyValue attr = attributes.get(i);
            if (attr.getKey().equals(ELASTICSEARCH_INDEX)) {
                index = attr.getValue().getStringValue();
            }
            if (isPopulated() == false && dataset == null && attr.getKey().equals(DATA_STREAM_DATASET)) {
                dataset = attr.getValue().getStringValue();
            }
            if (isPopulated() == false && namespace == null && attr.getKey().equals(DATA_STREAM_NAMESPACE)) {
                namespace = attr.getValue().getStringValue();
            }
            if (isPopulated()) {
                return;
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
}
