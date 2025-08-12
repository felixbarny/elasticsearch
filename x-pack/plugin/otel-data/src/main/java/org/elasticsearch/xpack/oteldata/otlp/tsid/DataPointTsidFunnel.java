/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.tsid;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.cluster.routing.TsidBuilder.TsidFunnel;
import org.elasticsearch.xpack.oteldata.otlp.DataPointGroupingContext;

public class DataPointTsidFunnel implements TsidFunnel<DataPointGroupingContext.DataPoint> {

    private static final DataPointTsidFunnel INSTANCE = new DataPointTsidFunnel();

    private DataPointTsidFunnel() {}

    public static TsidBuilder forDataPoint(DataPointGroupingContext.DataPoint dataPoint) {
        TsidBuilder tsidBuilder = new TsidBuilder(dataPoint.getAttributes().size() + 3);
        INSTANCE.add(dataPoint, tsidBuilder);
        return tsidBuilder;
    }

    @Override
    public void add(DataPointGroupingContext.DataPoint dataPoint, TsidBuilder tsidBuilder) {
        tsidBuilder.addLongDimension("@timestamp", dataPoint.getTimestampUnixNano());
        tsidBuilder.add(dataPoint, DataPointDimensionsTsidFunnel.get());
        tsidBuilder.addLongDimension("start_timestamp", dataPoint.getStartTimestampUnixNano());
    }
}
