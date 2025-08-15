/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.tsid;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.cluster.routing.TsidBuilder.TsidFunnel;
import org.elasticsearch.xpack.oteldata.otlp.MappingHints;
import org.elasticsearch.xpack.oteldata.otlp.TargetIndex;

import java.util.List;

class AttributeListTsidFunnel implements TsidFunnel<List<KeyValue>> {

    private final String prefix;

    private AttributeListTsidFunnel(String prefix) {
        this.prefix = prefix;
    }

    static AttributeListTsidFunnel get(String prefix) {
        return new AttributeListTsidFunnel(prefix);
    }

    @Override
    public void add(List<KeyValue> attributesList, TsidBuilder tsidBuilder) {
        for (int i = 0; i < attributesList.size(); i++) {
            KeyValue keyValue = attributesList.get(i);
            String attributeKey = keyValue.getKey();
            if (attributeKey.equals(TargetIndex.ELASTICSEARCH_INDEX)
                || attributeKey.equals(TargetIndex.DATA_STREAM_DATASET)
                || attributeKey.equals(TargetIndex.DATA_STREAM_NAMESPACE)
                || attributeKey.equals(MappingHints.MAPPING_HINTS)) {
                // ignore
                continue;
            }
            hashValue(tsidBuilder, prefix + attributeKey, keyValue.getValue());
        }
    }

    private void hashValue(TsidBuilder tsidBuilder, String key, AnyValue value) {
        switch (value.getValueCase()) {
            case STRING_VALUE -> tsidBuilder.addStringDimension(key, value.getStringValueBytes().toByteArray());
            case BOOL_VALUE -> tsidBuilder.addBooleanDimension(key, value.getBoolValue());
            case BYTES_VALUE -> tsidBuilder.addBytesDimension(key, value.getBytesValue().toByteArray());
            case DOUBLE_VALUE -> tsidBuilder.addDoubleDimension(key, value.getDoubleValue());
            case INT_VALUE -> tsidBuilder.addLongDimension(key, value.getIntValue());
            case KVLIST_VALUE -> tsidBuilder.add(value.getKvlistValue().getValuesList(), AttributeListTsidFunnel.get(key + "."));
            case ARRAY_VALUE -> {
                List<AnyValue> valuesList = value.getArrayValue().getValuesList();
                for (int i = 0; i < valuesList.size(); i++) {
                    hashValue(tsidBuilder, key, valuesList.get(i));
                }
            }
        }
    }
}
