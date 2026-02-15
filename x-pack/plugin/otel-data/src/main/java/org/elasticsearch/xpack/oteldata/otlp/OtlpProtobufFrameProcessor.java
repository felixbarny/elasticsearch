/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;

/**
 * Consumes framed OTLP protobuf payloads and produces a {@link AbstractOTLPTransportAction.ProcessingContext}
 * used to execute the transport action.
 * <p>
 * A processor instance is used for a single request lifecycle: repeated calls to {@link #onFrame(BytesReference)}
 * followed by exactly one terminal call to either {@link #onComplete()} or {@link #onFailure(Exception)}.
 */
public interface OtlpProtobufFrameProcessor {

    /**
     * Handles one framed protobuf payload.
     * <p>
     * The caller retains ownership of {@code frame} and releases it after this method returns.
     *
     * @param frame one framed OTLP protobuf payload
     * @throws IOException if the frame cannot be parsed or materialized
     */
    void onFrame(BytesReference frame) throws IOException;

    /**
     * Called exactly once when all frames are processed successfully.
     *
     * @return the processing context containing grouped data, prepared bulk request state, and any metadata for response generation
     */
    AbstractOTLPTransportAction.ProcessingContext onComplete();

    /**
     * Called exactly once when frame processing is aborted by a failure.
     *
     * @param failure the failure that aborted frame processing
     * @return the processing context updated with failure state for transport response handling
     */
    AbstractOTLPTransportAction.ProcessingContext onFailure(Exception failure);

    /**
     * Returns the number of data points processed so far, even if processing has not completed.
     * Used by the chunk handler to populate fallback error responses when {@link #onComplete()} or
     * {@link #onFailure(Exception)} themselves throw.
     */
    int totalDataPoints();
}
