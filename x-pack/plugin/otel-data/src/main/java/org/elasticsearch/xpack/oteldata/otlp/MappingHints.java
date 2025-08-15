/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;

import java.util.List;

public record MappingHints(boolean aggregateMetricDouble, boolean docCount) {
    public static final String MAPPING_HINTS = "elasticsearch.mapping.hints";

    private static final MappingHints EMPTY = new MappingHints(false, false);
    private static final String AGGREGATE_METRIC_DOUBLE = "aggregate_metric_double";
    private static final String DOC_COUNT = "_doc_count";

    public static MappingHints fromAttributes(List<KeyValue> attributes) {
        boolean aggregateMetricDouble = false;
        boolean docCount = false;
        for (KeyValue attribute : attributes) {
            if (attribute.getKey().equals(MAPPING_HINTS)) {
                if (attribute.getValue().hasArrayValue()) {
                    for (AnyValue hint : attribute.getValue().getArrayValue().getValuesList()) {
                        if (hint.hasStringValue()) {
                            switch (hint.getStringValue()) {
                                case AGGREGATE_METRIC_DOUBLE -> aggregateMetricDouble = true;
                                case DOC_COUNT -> docCount = true;
                            }
                        }
                    }
                }
                return new MappingHints(aggregateMetricDouble, docCount);
            }
        }
        return EMPTY;
    }
}
