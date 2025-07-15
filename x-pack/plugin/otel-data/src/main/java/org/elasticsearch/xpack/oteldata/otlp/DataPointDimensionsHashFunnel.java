/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import com.dynatrace.hash4j.hashing.HashFunnel;
import com.dynatrace.hash4j.hashing.HashSink;

public class DataPointDimensionsHashFunnel implements HashFunnel<OTLPMetricsTransportAction.DataPoint> {

    private static final DataPointDimensionsHashFunnel INSTANCE = new DataPointDimensionsHashFunnel();

    private DataPointDimensionsHashFunnel() {}

    public static DataPointDimensionsHashFunnel get() {
        return INSTANCE;
    }

    @Override
    public void put(OTLPMetricsTransportAction.DataPoint dataPoint, HashSink hashSink) {
        hashSink.putString(dataPoint.getUnit());
        hashSink.put(dataPoint.getAttributes(), AttributeListHashFunnel.get());
    }
}
