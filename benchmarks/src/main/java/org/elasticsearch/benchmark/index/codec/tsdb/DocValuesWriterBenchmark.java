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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(value = Mode.Throughput)
@State(value = Scope.Benchmark)
public class DocValuesWriterBenchmark {

    private Directory directory;
    private IndexWriter iwriter;
    @Param({ "1000" })
    private int numDocsPerIteration;
    @Param({ "0", "2", "10", "20" })
    private int dimensions;
    @Param({ "16" })
    private double indexingBufferMb;
    private Document[] documents;
    private Path path;

    @Setup
    public void setup() throws IOException {
        path = Path.of(System.getProperty("tests.index"));
        IOUtils.rm(path);
        directory = new MMapDirectory(path);
        iwriter = new IndexWriter(
            directory,
            new IndexWriterConfig(new StandardAnalyzer())
                // .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                .setRAMBufferSizeMB(indexingBufferMb)
                .setIndexSort(
                    new Sort(
                        new SortedNumericSortField("_tsid", SortField.Type.LONG),
                        new SortedNumericSortField("@timestamp", SortField.Type.LONG, true)
                    )
                )
        );
        Random random = new Random();
        Map<String, String> resourceAttributes = new HashMap<>();
        for (int i = 0; i < dimensions; i++) {
            resourceAttributes.put("resource.attributes.attr-" + i, "value-" + i);
        }
        long tsid = resourceAttributes.hashCode();
        documents = new Document[numDocsPerIteration];
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
            documents[i] = doc;
        }
    }

    @TearDown
    public void tearDown() throws Exception {
        if (iwriter != null) {
            iwriter.commit();
            iwriter.close();
        }
        if (directory != null) {
            directory.close();
        }
        IOUtils.rm(path);
    }

    @Benchmark
    public void benchmarkOneMetricPerDocument() throws IOException {
        for (int i = 0; i < numDocsPerIteration; i++) {
            iwriter.addDocument(documents[i]);
        }
    }

}
