/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.oteldata.otlp.proto.IncrementalProtobufParser;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Incremental OTLP protobuf chunk handler with an async single-consumer model.
 * <p>
 * Transport-thread work is reduced to queueing full chunks in {@link #handleChunk}.
 * A worker on {@link #executor} has sole ownership of dequeue, protobuf frame extraction, frame processing, releasing processed chunks,
 * and invoking the transport action on completion.
 */
final class OtlpProtobufFrameChunkHandler implements BaseRestHandler.RequestBodyChunkConsumer {

    private static final String OTLP_PROTOBUF_CONTENT_TYPE = "application/x-protobuf";
    static final String EXECUTOR = ThreadPool.Names.WRITE_COORDINATION;

    private final RestRequest request;
    private final NodeClient client;
    private final ActionType<OTLPActionResponse> actionType;
    private final OtlpProtobufFrameProcessor frameProcessor;
    private final IncrementalProtobufParser protobufParser;

    // Shared between transport thread (handleChunk) and worker.
    // workerRunning ensures there is at most one queue consumer;
    // a CAS from false->true is responsible for scheduling or continuing queue draining.
    private final Queue<QueueEntry> pendingChunks = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean workerRunning = new AtomicBoolean();
    private final ExecutorService executor;
    private volatile RestChannel channel;
    private volatile boolean closed;

    // Queue-consumer state: normally owned by processQueue/processChunk, which run single-threaded under workerRunning.
    // The only producer-side touch is failClosed() when worker scheduling fails after winning the false->true CAS;
    // in that path no worker is running, so access is still serialized.
    private final ArrayDeque<ReleasableBytesReference> unparsedData = new ArrayDeque<>();
    private boolean transportExecuted;
    private Exception failure;

    /**
     * Immutable queue payload produced on the transport thread and consumed by the worker.
     * Keeps per-chunk metadata close to the chunk to avoid coordinating separate structures.
     */
    private record QueueEntry(ReleasableBytesReference chunk, boolean isLast) {}

    OtlpProtobufFrameChunkHandler(
        RestRequest request,
        NodeClient client,
        ActionType<OTLPActionResponse> actionType,
        OtlpProtobufFrameProcessor frameProcessor,
        int framedFieldNumber
    ) {
        this.request = request;
        this.client = client;
        this.actionType = actionType;
        this.frameProcessor = frameProcessor;
        this.protobufParser = new IncrementalProtobufParser(framedFieldNumber);
        this.executor = client.threadPool().executor(EXECUTOR);
    }

    @Override
    public void accept(RestChannel channel) {
        this.channel = channel;
        if (request.isStreamedContent()) {
            // Prime the first item so subsequent next() calls from processChunk advance correctly.
            request.contentStream().next();
        }
    }

    /**
     * Callback that receives chunks of the request body as they become available.
     * Chunks are queued and processed asynchronously on a worker from the {@link #executor}
     * to allow the transport thread to return to reading the next chunk as soon as possible.
     *
     * @param channel The rest channel associated to the request
     * @param chunk The chunk of request body that is ready for processing
     * @param isLast Whether the chunk is the last one of the request
     */
    @Override
    public void handleChunk(RestChannel channel, ReleasableBytesReference chunk, boolean isLast) {
        assert Transports.assertTransportThread();
        assert this.channel == channel;
        if (closed) {
            chunk.close();
            return;
        }
        pendingChunks.add(new QueueEntry(chunk, isLast));
        if (workerRunning.compareAndSet(false, true)) {
            try {
                executor.execute(this::processQueue);
            } catch (Exception e) {
                workerRunning.set(false);
                failClosed(e, true);
            }
        }
    }

    @Override
    public void streamClose() {
        assert Transports.assertTransportThread();
        // Best-effort early termination signal for future chunks and worker iterations.
        closed = true;
    }

    /**
     * Worker loop that runs on the {@link #executor} thread pool.
     * <p>
     * This drains all currently available chunks, then performs a lost-wakeup check before exit so chunks
     * queued concurrently with worker handoff are not left unprocessed.
     */
    private void processQueue() {
        do {
            drainQueuedChunks();
        } while (tryContinueDraining());
    }

    private void drainQueuedChunks() {
        QueueEntry entry;
        while ((entry = pendingChunks.poll()) != null) {
            try {
                processChunk(entry);
            } catch (Exception e) {
                if (isBuffered(entry.chunk()) == false) {
                    entry.chunk().close();
                }
                failClosed(e, true);
            }
        }
    }

    /*
     * Lost-wakeup check to handle the case where a producer enqueues a chunk after the last poll returned null but before this worker
     * exits and sets workerRunning=false.
     * If that happens, the producer will not schedule a new worker since it observed workerRunning=true,
     * so this check ensures that the newly enqueued chunk will be processed by this same worker before exit.
     * The CAS also serves to re-establish the workerRunning invariant for the next producer.
     */
    private boolean tryContinueDraining() {
        workerRunning.set(false);
        return pendingChunks.isEmpty() == false && workerRunning.compareAndSet(false, true);
    }

    /**
     * Processes a single queued chunk on the worker.
     * <p>
     * This method advances the streaming source, applies fast-fail/early-return checks, appends
     * bytes for incremental parsing, and triggers transport execution on the final chunk.
     */
    private void processChunk(QueueEntry entry) throws Exception {
        assert Transports.assertNotTransportThread("OTLP chunk processing should not run on transport threads");

        if (closed) {
            entry.chunk().close();
            Releasables.close(unparsedData);
            unparsedData.clear();
            return;
        }

        if (entry.isLast() == false && request.isStreamedContent()) {
            request.contentStream().next();
        }

        if (transportExecuted) {
            entry.chunk().close();
            return;
        }

        if (failure != null) {
            entry.chunk().close();
            if (entry.isLast()) {
                executeTransport();
            }
            return;
        }

        if (entry.chunk().length() == 0) {
            entry.chunk().close();
        } else {
            unparsedData.add(entry.chunk());
        }

        BytesReference data = toComposite();
        if (data != null) {
            int consumed = protobufParser.parse(data, entry.isLast(), frameProcessor::onFrame);
            releaseConsumedBytes(consumed);
        }

        if (entry.isLast()) {
            executeTransport();
        }
    }

    /**
     * Returns either the single buffered chunk or a composite view over all buffered chunks.
     * The returned reference is zero-copy and only valid while the backing chunk queue is retained.
     */
    private BytesReference toComposite() {
        if (unparsedData.isEmpty()) {
            return null;
        }
        if (unparsedData.size() == 1) {
            return unparsedData.peek();
        }
        return CompositeBytesReference.of(unparsedData.toArray(new BytesReference[0]));
    }

    private boolean isBuffered(ReleasableBytesReference chunk) {
        for (ReleasableBytesReference buffered : unparsedData) {
            if (buffered == chunk) {
                return true;
            }
        }
        return false;
    }

    private void failClosed(Exception e, boolean executeTransportNow) {
        if (failure == null) {
            failure = e;
        } else if (failure != e) {
            failure.addSuppressed(e);
        }
        closed = true;
        Releasables.close(unparsedData);
        unparsedData.clear();
        releaseQueuedChunks();
        if (executeTransportNow) {
            executeTransport();
        }
    }

    private void releaseQueuedChunks() {
        QueueEntry queued;
        while ((queued = pendingChunks.poll()) != null) {
            queued.chunk().close();
        }
    }

    /**
     * Finalizes frame processing and sends exactly one transport action request.
     * <p>
     * If parsing or frame materialization failed, {@link OtlpProtobufFrameProcessor#onFailure(Exception)}
     * is used to build an error context; otherwise {@link OtlpProtobufFrameProcessor#onComplete()} provides
     * the successful context payload.
     */
    private void executeTransport() {
        if (transportExecuted) {
            return;
        }
        transportExecuted = true;
        AbstractOTLPTransportAction.ProcessingContext processingContext;
        OTLPActionRequest actionRequest;
        try {
            processingContext = failure != null ? frameProcessor.onFailure(failure) : frameProcessor.onComplete();
            actionRequest = new OTLPActionRequest(processingContext);
        } catch (Exception e) {
            if (failure != null) {
                failure.addSuppressed(e);
            } else {
                failure = e;
            }
            actionRequest = new OTLPActionRequest(
                AbstractOTLPTransportAction.ProcessingContext.failureOnly(frameProcessor.totalDataPoints(), failure)
            );
        }
        client.execute(actionType, actionRequest, new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(OTLPActionResponse response) {
                return new RestResponse(response.getStatus(), OTLP_PROTOBUF_CONTENT_TYPE, response.getResponse());
            }
        });
    }

    /**
     * Releases fully consumed chunks from the front of {@link #unparsedData}, retaining
     * a partial slice if a chunk boundary falls inside a consumed region.
     */
    private void releaseConsumedBytes(int bytesConsumed) {
        while (bytesConsumed > 0 && unparsedData.isEmpty() == false) {
            ReleasableBytesReference head = unparsedData.peekFirst();
            if (bytesConsumed >= head.length()) {
                bytesConsumed -= head.length();
                unparsedData.removeFirst().close();
            } else {
                ReleasableBytesReference remaining = head.retainedSlice(bytesConsumed, head.length() - bytesConsumed);
                unparsedData.removeFirst().close();
                unparsedData.addFirst(remaining);
                bytesConsumed = 0;
            }
        }
        assert bytesConsumed == 0 : "parser reported consuming more bytes than available in chunk queue";
    }
}
