/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This class constructs an Elasticsearch document representation of a metric data point group.
 * It also handles dynamic templates for metrics based on their attributes.
 */
public class MetricDocumentBuilder extends OTelDocumentBuilder {

    private final BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0);

    public MetricDocumentBuilder(BufferedByteStringAccessor byteStringAccessor) {
        super(byteStringAccessor);
    }

    public HashMap<String, String> buildMetricDocument(XContentBuilder builder, DataPointGroupingContext.DataPointGroup dataPointGroup)
        throws IOException {
        HashMap<String, String> dynamicTemplates = new HashMap<>();
        List<DataPoint> dataPoints = dataPointGroup.dataPoints();
        builder.startObject();
        builder.field("@timestamp", TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getTimestampUnixNano()));
        if (dataPointGroup.getStartTimestampUnixNano() != 0) {
            builder.field("start_timestamp", TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getStartTimestampUnixNano()));
        }
        buildResource(dataPointGroup.resource(), dataPointGroup.resourceSchemaUrl(), builder);
        buildDataStream(builder, dataPointGroup.targetIndex());
        buildScope(builder, dataPointGroup.scope(), dataPointGroup.scopeSchemaUrl());
        buildAttributes(builder, dataPointGroup.dataPointAttributes(), 0);
        if (Strings.hasLength(dataPointGroup.unit())) {
            builder.field("unit", dataPointGroup.unit());
        }
        builder.field("_metric_names_hash", dataPointGroup.getMetricNamesHash(hasher));

        long docCount = 0;
        builder.startObject("metrics");
        for (int i = 0, dataPointsSize = dataPoints.size(); i < dataPointsSize; i++) {
            DataPoint dataPoint = dataPoints.get(i);
            builder.field(dataPoint.getMetricName());
            MappingHints mappingHints = MappingHints.fromAttributes(dataPoint.getAttributes());
            dataPoint.buildMetricValue(mappingHints, builder);
            String dynamicTemplate = dataPoint.getDynamicTemplate(mappingHints);
            if (dynamicTemplate != null) {
                dynamicTemplates.put("metrics." + dataPoint.getMetricName(), dynamicTemplate);
            }
            if (mappingHints.docCount()) {
                docCount = dataPoint.getDocCount();
            }
        }
        builder.endObject();
        if (docCount > 0) {
            builder.field("_doc_count", docCount);
        }
        builder.endObject();
        return dynamicTemplates;
    }

}
