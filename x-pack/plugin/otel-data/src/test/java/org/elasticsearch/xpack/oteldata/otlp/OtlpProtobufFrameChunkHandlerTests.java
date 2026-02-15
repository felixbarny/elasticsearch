/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.Metric;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeHttpBodyStream;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.startsWith;

public class OtlpProtobufFrameChunkHandlerTests extends ESTestCase {

    private static final ActionType<OTLPActionResponse> TEST_ACTION = new ActionType<>("indices:data/write/otlp/metrics");
    private static final int RESOURCE_METRICS_FIELD_NUMBER = 1;

    @Before
    public void setFakeThreadName() {
        // to satisfy checks that we're running on a transport thread
        Thread.currentThread().setName(Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX + Thread.currentThread().getName());
    }

    @After
    public void resetThreadName() {
        final var threadName = Thread.currentThread().getName();
        assertThat(threadName, startsWith(Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX));
        Thread.currentThread().setName(threadName.substring(Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX.length()));
    }

    public void testConsumesFramedMetricsAndExecutesTransport() throws Exception {
        try (var threadPool = createThreadPool()) {
            CapturingNodeClient client = new CapturingNodeClient(threadPool);
            FakeRestRequest restRequest = createFullRequest();
            OtlpProtobufFrameChunkHandler chunkHandler = new OtlpProtobufFrameChunkHandler(
                restRequest,
                client,
                TEST_ACTION,
                new OTLPMetricsFrameProcessor(MappingHints.DEFAULT_TDIGEST, client),
                RESOURCE_METRICS_FIELD_NUMBER
            );
            RestChannel channel = new FakeRestChannel(restRequest, randomBoolean(), 1);

            chunkHandler.accept(channel);
            chunkHandler.handleChunk(channel, metricsChunk(), true);

            assertBusy(() -> assertThat(client.executeCount.get(), equalTo(1)));
            assertThat(client.capturedRequest.getProcessingContext().getBulkRequestBuilder().numberOfActions(), greaterThan(0));
            assertThat(client.capturedRequest.getProcessingContext().totalDataPoints(), greaterThan(0));
        }
    }

    public void testPrefetchRequestsNextChunkWhileCurrentChunkIsProcessing() throws Exception {
        try (var threadPool = createThreadPool()) {
            CapturingNodeClient client = new CapturingNodeClient(threadPool);
            CountingHttpBodyStream bodyStream = new CountingHttpBodyStream();
            FakeRestRequest request = createStreamedRequest(bodyStream);

            CountDownLatch blockProcessor = new CountDownLatch(1);
            TestFrameProcessor processor = new TestFrameProcessor(client);
            processor.blockOnFrame = blockProcessor;

            BaseRestHandler.RequestBodyChunkConsumer chunkHandler = createChunkHandler(request, client, processor);
            RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);

            chunkHandler.accept(channel);
            assertThat(bodyStream.nextCalls.get(), equalTo(1));

            chunkHandler.handleChunk(channel, metricsChunk(), false);
            assertBusy(() -> assertThat(bodyStream.nextCalls.get(), equalTo(2)));

            blockProcessor.countDown();
            assertBusy(() -> assertThat(bodyStream.nextCalls.get(), equalTo(2)));
        }
    }

    public void testWorkerRestartsWhenChunkArrivesAfterIdle() throws Exception {
        try (var threadPool = createThreadPool()) {
            CapturingNodeClient client = new CapturingNodeClient(threadPool);
            CountingHttpBodyStream bodyStream = new CountingHttpBodyStream();
            FakeRestRequest request = createStreamedRequest(bodyStream);

            TestFrameProcessor processor = new TestFrameProcessor(client);

            BaseRestHandler.RequestBodyChunkConsumer chunkHandler = createChunkHandler(request, client, processor);
            RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);

            chunkHandler.accept(channel);
            chunkHandler.handleChunk(channel, metricsChunk(), false);

            assertBusy(() -> {
                assertThat(processor.frameCalls.get(), equalTo(1));
                assertThat(bodyStream.nextCalls.get(), equalTo(2));
            });

            chunkHandler.handleChunk(channel, metricsChunk(), true);
            assertBusy(() -> {
                assertThat(processor.frameCalls.get(), equalTo(2));
                assertThat(client.executeCount.get(), equalTo(1));
            });
        }
    }

    public void testDrainsMultiplePendingChunks() throws Exception {
        try (var threadPool = createThreadPool()) {
            CapturingNodeClient client = new CapturingNodeClient(threadPool);
            CountingHttpBodyStream bodyStream = new CountingHttpBodyStream();
            FakeRestRequest request = createStreamedRequest(bodyStream);

            CountDownLatch blockFirstFrame = new CountDownLatch(1);
            TestFrameProcessor processor = new TestFrameProcessor(client);
            processor.blockOnFrame = blockFirstFrame;

            BaseRestHandler.RequestBodyChunkConsumer chunkHandler = createChunkHandler(request, client, processor);
            RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);

            chunkHandler.accept(channel);
            chunkHandler.handleChunk(channel, metricsChunk(), false);
            assertBusy(() -> assertThat(processor.frameCalls.get(), equalTo(1)));

            int extraChunks = 16;
            for (int i = 0; i < extraChunks; i++) {
                chunkHandler.handleChunk(channel, metricsChunk(), false);
            }
            chunkHandler.handleChunk(channel, metricsChunk(), true);

            assertThat(client.executeCount.get(), equalTo(0));

            blockFirstFrame.countDown();
            int expectedFrames = extraChunks + 2;
            assertBusy(() -> {
                assertThat(processor.frameCalls.get(), equalTo(expectedFrames));
                assertThat(client.executeCount.get(), equalTo(1));
            });
            assertNotNull(client.capturedRequest);
            assertNull(client.capturedRequest.getProcessingContext().getFailure());
        }
    }

    public void testOnFrameFailureShortCircuitsAndExecutesTransportOnce() throws Exception {
        try (var threadPool = createThreadPool()) {
            CapturingNodeClient client = new CapturingNodeClient(threadPool);
            FakeRestRequest restRequest = createFullRequest();
            AtomicBoolean onCompleteCalled = new AtomicBoolean();
            OtlpProtobufFrameProcessor failingProcessor = new OtlpProtobufFrameProcessor() {
                @Override
                public void onFrame(BytesReference frame) throws IOException {
                    throw new IllegalStateException("onFrame boom");
                }

                @Override
                public AbstractOTLPTransportAction.ProcessingContext onComplete() {
                    onCompleteCalled.set(true);
                    return newProcessingContext(client, null);
                }

                @Override
                public AbstractOTLPTransportAction.ProcessingContext onFailure(Exception failure) {
                    return newProcessingContext(client, failure);
                }

                @Override
                public int totalDataPoints() {
                    return 0;
                }
            };
            BaseRestHandler.RequestBodyChunkConsumer chunkHandler = new OtlpProtobufFrameChunkHandler(
                restRequest,
                client,
                TEST_ACTION,
                failingProcessor,
                RESOURCE_METRICS_FIELD_NUMBER
            );
            RestChannel channel = new FakeRestChannel(restRequest, randomBoolean(), 1);

            chunkHandler.accept(channel);
            chunkHandler.handleChunk(channel, metricsChunk(), true);

            assertBusy(() -> assertThat(client.executeCount.get(), equalTo(1)));
            assertThat(onCompleteCalled.get(), equalTo(false));
            assertNotNull(client.capturedRequest);
            assertNotNull(client.capturedRequest.getProcessingContext().getFailure());
            assertThat(client.capturedRequest.getProcessingContext().getFailure().getMessage(), containsString("onFrame boom"));

            // Second handleChunk after transport already executed should not execute again.
            chunkHandler.handleChunk(channel, metricsChunk(), true);
            assertThat(client.executeCount.get(), equalTo(1));
        }
    }

    public void testOnCompleteFailureInvokesOnFailure() throws Exception {
        try (var threadPool = createThreadPool()) {
            CapturingNodeClient client = new CapturingNodeClient(threadPool);
            FakeRestRequest restRequest = createFullRequest();
            OtlpProtobufFrameProcessor processor = new OtlpProtobufFrameProcessor() {
                @Override
                public void onFrame(BytesReference frame) {}

                @Override
                public AbstractOTLPTransportAction.ProcessingContext onComplete() {
                    throw new IllegalStateException("onComplete boom");
                }

                @Override
                public AbstractOTLPTransportAction.ProcessingContext onFailure(Exception failure) {
                    return newProcessingContext(client, failure);
                }

                @Override
                public int totalDataPoints() {
                    return 0;
                }
            };
            BaseRestHandler.RequestBodyChunkConsumer chunkHandler = new OtlpProtobufFrameChunkHandler(
                restRequest,
                client,
                TEST_ACTION,
                processor,
                RESOURCE_METRICS_FIELD_NUMBER
            );
            RestChannel channel = new FakeRestChannel(restRequest, randomBoolean(), 1);

            chunkHandler.accept(channel);
            chunkHandler.handleChunk(channel, metricsChunk(), true);

            assertBusy(() -> assertThat(client.executeCount.get(), equalTo(1)));
        }
    }

    public void testFailureOnMiddleChunkFailsClosedAndExecutesTransportOnce() throws Exception {
        try (var threadPool = createThreadPool()) {
            CapturingNodeClient client = new CapturingNodeClient(threadPool);
            CountingHttpBodyStream bodyStream = new CountingHttpBodyStream();
            FakeRestRequest request = createStreamedRequest(bodyStream);

            AtomicBoolean shouldFail = new AtomicBoolean(true);
            TestFrameProcessor processor = new TestFrameProcessor(client) {
                @Override
                public void onFrame(BytesReference frame) throws IOException {
                    if (shouldFail.get()) {
                        throw new IllegalStateException("mid-stream failure");
                    }
                    super.onFrame(frame);
                }
            };

            BaseRestHandler.RequestBodyChunkConsumer chunkHandler = createChunkHandler(request, client, processor);
            RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);

            chunkHandler.accept(channel);
            // First chunk fails during frame processing.
            chunkHandler.handleChunk(channel, metricsChunk(), false);
            assertBusy(() -> assertThat(client.executeCount.get(), equalTo(1)));
            assertThat(bodyStream.nextCalls.get(), equalTo(2));
            assertNotNull(client.capturedRequest.getProcessingContext().getFailure());
            assertThat(client.capturedRequest.getProcessingContext().getFailure().getMessage(), containsString("mid-stream failure"));

            // Subsequent chunks should be closed without processing and without re-executing transport.
            shouldFail.set(false);
            chunkHandler.handleChunk(channel, metricsChunk(), false);
            chunkHandler.handleChunk(channel, metricsChunk(), true);
            assertThat(processor.frameCalls.get(), equalTo(0));
            assertThat(bodyStream.nextCalls.get(), equalTo(2));
            assertThat(client.executeCount.get(), equalTo(1));
        }
    }

    public void testExecutorRejectionFailsClosedExecutesTransportAndClosesChunks() throws Exception {
        try (var threadPool = new RejectingOtlpExecutorTestThreadPool()) {
            CapturingNodeClient client = new CapturingNodeClient(threadPool);
            FakeRestRequest request = createFullRequest();
            TestFrameProcessor processor = new TestFrameProcessor(client);
            BaseRestHandler.RequestBodyChunkConsumer chunkHandler = createChunkHandler(request, client, processor);
            RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);

            AtomicBoolean firstChunkReleased = new AtomicBoolean();
            ReleasableBytesReference firstChunk = new ReleasableBytesReference(
                new BytesArray(metricsBytes()),
                () -> firstChunkReleased.set(true)
            );

            AtomicBoolean secondChunkReleased = new AtomicBoolean();
            ReleasableBytesReference secondChunk = new ReleasableBytesReference(
                new BytesArray(metricsBytes()),
                () -> secondChunkReleased.set(true)
            );

            chunkHandler.accept(channel);
            chunkHandler.handleChunk(channel, firstChunk, false);
            assertThat(client.executeCount.get(), equalTo(1));
            assertTrue(firstChunkReleased.get());
            assertNotNull(client.capturedRequest);
            assertNotNull(client.capturedRequest.getProcessingContext().getFailure());

            chunkHandler.handleChunk(channel, secondChunk, true);
            assertTrue(secondChunkReleased.get());
            assertThat(client.executeCount.get(), equalTo(1));
            assertThat(processor.frameCalls.get(), equalTo(0));
        }
    }

    public void testStreamCloseReleasesResourcesWithoutTransport() throws Exception {
        try (var threadPool = createThreadPool()) {
            CapturingNodeClient client = new CapturingNodeClient(threadPool);
            CountingHttpBodyStream bodyStream = new CountingHttpBodyStream();
            FakeRestRequest request = createStreamedRequest(bodyStream);

            CountDownLatch blockFrame = new CountDownLatch(1);
            TestFrameProcessor processor = new TestFrameProcessor(client);
            processor.blockOnFrame = blockFrame;

            BaseRestHandler.RequestBodyChunkConsumer chunkHandler = createChunkHandler(request, client, processor);
            RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);

            chunkHandler.accept(channel);
            chunkHandler.handleChunk(channel, metricsChunk(), false);
            assertBusy(() -> assertThat(processor.frameCalls.get(), equalTo(1)));

            // Enqueue another chunk, then close the stream before unblocking.
            chunkHandler.handleChunk(channel, metricsChunk(), false);
            chunkHandler.streamClose();
            blockFrame.countDown();

            // Give the worker time to drain; verify no transport execution.
            assertBusy(() -> assertThat(bodyStream.nextCalls.get(), greaterThan(1)));
            assertThat(client.executeCount.get(), equalTo(0));
        }
    }

    public void testHandleChunkAfterStreamCloseClosesChunk() throws Exception {
        try (var threadPool = createThreadPool()) {
            CapturingNodeClient client = new CapturingNodeClient(threadPool);
            FakeRestRequest restRequest = createFullRequest();
            TestFrameProcessor processor = new TestFrameProcessor(client);
            BaseRestHandler.RequestBodyChunkConsumer chunkHandler = createChunkHandler(restRequest, client, processor);
            RestChannel channel = new FakeRestChannel(restRequest, randomBoolean(), 1);

            chunkHandler.accept(channel);
            chunkHandler.streamClose();

            AtomicBoolean chunkReleased = new AtomicBoolean();
            ReleasableBytesReference chunk = new ReleasableBytesReference(new BytesArray(metricsBytes()), () -> chunkReleased.set(true));
            chunkHandler.handleChunk(channel, chunk, true);

            assertTrue(chunkReleased.get());
            assertThat(client.executeCount.get(), equalTo(0));
            assertThat(processor.frameCalls.get(), equalTo(0));
        }
    }

    // -- helpers --

    private FakeRestRequest createStreamedRequest(CountingHttpBodyStream bodyStream) {
        HashMap<String, List<String>> headers = new HashMap<>();
        headers.put("Content-Type", List.of("application/x-protobuf"));
        return new FakeRestRequest.Builder(xContentRegistry()).withPath("/_otlp/v1/metrics")
            .withMethod(RestRequest.Method.POST)
            .withHeaders(headers)
            .withContentLength(1)
            .withBody(bodyStream)
            .build();
    }

    private FakeRestRequest createFullRequest() {
        return new FakeRestRequest.Builder(xContentRegistry()).withPath("/_otlp/v1/metrics")
            .withMethod(RestRequest.Method.POST)
            .withContent(new BytesArray(new byte[] { 1 }), null)
            .withHeaders(java.util.Map.of("Content-Type", List.of("application/x-protobuf")))
            .build();
    }

    private static BaseRestHandler.RequestBodyChunkConsumer createChunkHandler(
        FakeRestRequest request,
        CapturingNodeClient client,
        OtlpProtobufFrameProcessor processor
    ) {
        return new OtlpProtobufFrameChunkHandler(request, client, TEST_ACTION, processor, RESOURCE_METRICS_FIELD_NUMBER);
    }

    private static byte[] metricsBytes() {
        Metric metric = OtlpUtils.createGaugeMetric("test.metric", "", List.of(OtlpUtils.createDoubleDataPoint(0)));
        return ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                OtlpUtils.createResourceMetrics(
                    List.of(keyValue("service.name", "test-service")),
                    List.of(OtlpUtils.createScopeMetrics("test", "1.0.0", List.of(metric)))
                )
            )
            .build()
            .toByteArray();
    }

    private static ReleasableBytesReference metricsChunk() {
        return new ReleasableBytesReference(new BytesArray(metricsBytes()), () -> {});
    }

    private static DataPointGroupingContext newProcessingContext(CapturingNodeClient client, Exception failure) {
        DataPointGroupingContext context = new DataPointGroupingContext(new BufferedByteStringAccessor(), client.prepareBulk());
        if (failure != null) {
            context.onFailure(failure);
        }
        return context;
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static class TestFrameProcessor implements OtlpProtobufFrameProcessor {
        final AtomicInteger frameCalls = new AtomicInteger();
        final CapturingNodeClient client;
        volatile CountDownLatch blockOnFrame;

        TestFrameProcessor(CapturingNodeClient client) {
            this.client = client;
        }

        @Override
        public void onFrame(BytesReference frame) throws IOException {
            frameCalls.incrementAndGet();
            CountDownLatch latch = blockOnFrame;
            if (latch != null) {
                blockOnFrame = null;
                await(latch);
            }
        }

        @Override
        public AbstractOTLPTransportAction.ProcessingContext onComplete() {
            return newProcessingContext(client, null);
        }

        @Override
        public AbstractOTLPTransportAction.ProcessingContext onFailure(Exception failure) {
            return newProcessingContext(client, failure);
        }

        @Override
        public int totalDataPoints() {
            return 0;
        }
    }

    private static class CapturingNodeClient extends NoOpNodeClient {
        private final AtomicInteger executeCount = new AtomicInteger();
        private volatile OTLPActionRequest capturedRequest;

        CapturingNodeClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            executeCount.incrementAndGet();
            capturedRequest = (OTLPActionRequest) request;
            @SuppressWarnings("unchecked")
            Response response = (Response) new OTLPActionResponse(RestStatus.OK, org.elasticsearch.common.bytes.BytesArray.EMPTY);
            listener.onResponse(response);
        }
    }

    private static class RejectingOtlpExecutorTestThreadPool extends TestThreadPool {
        private static final ExecutorService REJECTING_EXECUTOR = new AbstractExecutorService() {
            @Override
            public void shutdown() {}

            @Override
            public List<Runnable> shutdownNow() {
                return List.of();
            }

            @Override
            public boolean isShutdown() {
                return false;
            }

            @Override
            public boolean isTerminated() {
                return false;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) {
                return true;
            }

            @Override
            public void execute(Runnable command) {
                throw new EsRejectedExecutionException("simulated generic rejection", true);
            }
        };

        RejectingOtlpExecutorTestThreadPool() {
            super("rejecting-protobuf-parsing-threadpool");
        }

        @Override
        public ExecutorService executor(String name) {
            if (OtlpProtobufFrameChunkHandler.EXECUTOR.equals(name)) {
                return REJECTING_EXECUTOR;
            }
            return super.executor(name);
        }
    }

    private static class CountingHttpBodyStream extends FakeHttpBodyStream {
        private final AtomicInteger nextCalls = new AtomicInteger();

        @Override
        public void next() {
            nextCalls.incrementAndGet();
            super.next();
        }
    }
}
