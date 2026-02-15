/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class OTLPActionRequest extends ActionRequest implements CompositeIndicesRequest {
    private final AbstractOTLPTransportAction.ProcessingContext processingContext;

    public OTLPActionRequest(StreamInput in) throws IOException {
        super(in);
        this.processingContext = null;
        throw new UnsupportedOperationException("OTLPActionRequest only supports local execution and should not be serialized");
    }

    /**
     * Creates a local-only OTLP action request carrying request processing metadata and execution state.
     */
    public OTLPActionRequest(AbstractOTLPTransportAction.ProcessingContext processingContext) {
        this.processingContext = Objects.requireNonNull(processingContext);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("OTLPActionRequest only supports local execution and should not be serialized");
    }

    /**
     * Returns request processing metadata collected while parsing OTLP frames.
     */
    public AbstractOTLPTransportAction.ProcessingContext getProcessingContext() {
        return processingContext;
    }
}
