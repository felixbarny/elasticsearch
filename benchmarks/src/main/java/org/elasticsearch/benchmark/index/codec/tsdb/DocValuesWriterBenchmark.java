/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.logging.internal.LoggerFactoryImpl;
import org.elasticsearch.index.codec.Elasticsearch900Lucene101Codec;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.logging.internal.spi.LoggerFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(value = Mode.Throughput)
@State(value = Scope.Benchmark)
public class DocValuesWriterBenchmark {

    private static final int FLUSH_INTERVAL_SEC = 1;
    private final Object commitLock = new Object();

    private Directory directory;
    private IndexWriter iwriter;
    @Param({ "10000" })
    private int numDocsPerIteration;
    @Param({ "0", "2", "10", "20" })
    private int dimensions;
    @Param({ "16" })
    private double indexingBufferMb;
    private Document[] documents;
    private Path path;
    private ScheduledExecutorService executor;

    @Setup(Level.Iteration)
    public void setup() throws IOException {
        LoggerFactory.setInstance(new LoggerFactoryImpl());

        path = Path.of(System.getProperty("tests.index"));
        IOUtils.rm(path);
        directory = new MMapDirectory(path);
        var codec = new Elasticsearch900Lucene101Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return new ES819TSDBDocValuesFormat();
            }
        };
        var config = new IndexWriterConfig(new StandardAnalyzer()).setCodec(codec)
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            .setRAMBufferSizeMB(indexingBufferMb)
            .setIndexSort(
                new Sort(
                    new SortedNumericSortField("_tsid", SortField.Type.LONG),
                    new SortedNumericSortField("@timestamp", SortField.Type.LONG, true)
                )
            )
            .setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER)
            .setMergePolicy(new LogByteSizeMergePolicy());
        iwriter = new IndexWriter(directory, config);
        documents = initDocs();
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> {
            try {
                if (iwriter != null) {
                    synchronized (commitLock) {
                        iwriter.commit();
                    }
                }
            } catch (IOException ignore) {}
        }, FLUSH_INTERVAL_SEC, FLUSH_INTERVAL_SEC, TimeUnit.SECONDS);
    }

    private Document[] initDocs() {
        Random random = new Random();
        Map<String, String> resourceAttributes = new HashMap<>();
        for (int i = 0; i < dimensions; i++) {
            resourceAttributes.put("resource.attributes.attr-" + i, "value-" + i);
        }
        long tsid = resourceAttributes.hashCode();
        Document[] docs = new Document[numDocsPerIteration];
        long value = 1_000_000L;
        for (int i = 0; i < numDocsPerIteration; i++) {
            Document doc = new Document();
            doc.add(SortedNumericDocValuesField.indexedField("_tsid", tsid));
            doc.add(SortedNumericDocValuesField.indexedField("@timestamp", System.currentTimeMillis() + i));
            double incrementDouble = random.nextGaussian(100, 1);
            long incrmentLong = (long) incrementDouble;
            value += incrmentLong;
            // probabilistic rounding
            if (incrementDouble - incrmentLong > random.nextDouble()) {
                value++;
            }
            doc.add(new NumericDocValuesField("value", value));
            for (Map.Entry<String, String> entry : resourceAttributes.entrySet()) {
                doc.add(SortedDocValuesField.indexedField(entry.getKey(), new BytesRef(entry.getValue())));
            }
            docs[i] = doc;
        }
        return docs;
    }

    @TearDown(Level.Iteration)
    public void tearDown() throws Exception {
        executor.shutdown();
        if (iwriter != null) {
            synchronized (commitLock) {
                iwriter.commit();
                iwriter.close();
            }
        }
        if (directory != null) {
            directory.close();
        }
        IOUtils.rm(path);
    }

    @Benchmark
    @Threads(1)
    public void benchmarkOneMetricPerDocument1Thread() throws IOException {
        for (int i = 0; i < numDocsPerIteration; i++) {
            iwriter.addDocument(documents[i]);
        }
    }

    @Benchmark
    @Threads(4)
    public void benchmarkOneMetricPerDocument4Threads() throws IOException {
        for (int i = 0; i < numDocsPerIteration; i++) {
            iwriter.addDocument(documents[i]);
        }
    }

}
