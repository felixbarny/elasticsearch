/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.promql.parser;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.PromqlParser;

import java.io.BufferedReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Test for checking the overall grammar by throwing a number of valid queries at the parser to see whether any exception is raised.
 * In time, the queries themselves get to be checked against the actual execution model and eventually against the expected results.
 */
public class PromqlAstTests extends ESTestCase {

    public void testValidQueries() throws Exception {
        PromqlParser parser = new PromqlParser();
        List<Tuple<String, Integer>> lines = readQueries("/queries-valid.promql");
        for (Tuple<String, Integer> line : lines) {
            String q = line.v1();
            try {
                parser.createExpression(q);
            } catch (ParsingException pe) {
                fail(
                    format(null,
                        "Error parsing line {}:{} '{}' [{}]",
                        line.v2(),
                        pe.getColumnNumber(),
                        pe.getErrorMessage(),
                        q
                    )
                );
            } catch (Exception e) {
                fail(format(null, "Unexpected exception for line {}: [{}] \n {}", line.v2(), line.v1(), e));
            }
        }
    }

    public void testQuery() throws Exception {
        String query = "round(some_metric)";
        new PromqlParser().createExpression(query);
    }

    public void testUnsupportedQueries() throws Exception {
        PromqlParser parser = new PromqlParser();
        List<Tuple<String, Integer>> lines = readQueries("/queries-invalid.promql");
        for (Tuple<String, Integer> line : lines) {
            String q = line.v1();
            try {
                ParsingException pe = expectThrows(
                    ParsingException.class,
                    "No exception parsing line " + line.v2() + ":[" + q + "]",
                    () -> parser.createExpression(q)
                );
            } catch (AssertionError ae) {
                fail(format(null, "Unexpected exception for line {}: [{}] \n {}", line.v2(), line.v1(), ae.getCause()));
            }
        }
    }

    private static List<Tuple<String, Integer>> readQueries(String source) throws Exception {
        var urls = EsqlTestUtils.classpathResources(source);
        List<Tuple<String, Integer>> queries = new ArrayList<>();

        StringBuilder query = new StringBuilder();
        for (URL url : urls) {
            try (BufferedReader reader = EsqlTestUtils.reader(url)) {
                String line;
                int lineNumber = 1;

                while ((line = reader.readLine()) != null) {
                    // ignore comments
                    if (line.isEmpty() == false && line.startsWith("//") == false) {
                        query.append(line);

                        if (line.endsWith(";")) {
                            query.setLength(query.length() - 1);
                            queries.add(new Tuple<>(query.toString(), lineNumber));
                            query.setLength(0);
                        } else {
                            query.append("\n");
                        }
                    }
                    lineNumber++;
                }
            }
        }
        return queries;
    }
}
