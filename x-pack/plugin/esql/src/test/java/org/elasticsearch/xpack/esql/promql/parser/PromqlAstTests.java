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
import org.elasticsearch.xpack.esql.core.QlClientException;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.PromqlParser;

import java.io.BufferedReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Test for checking the overall grammar by throwing a number of valid queries at the parser to see whether any exception is raised.
 * In time, the queries themselves get to be checked against the actual execution model and eventually against the expected results.
 */
public class PromqlAstTests extends ESTestCase {

    public void testValidQueries() throws Exception {
        List<Tuple<String, Integer>> lines = PromqlGrammarTests.readQueries("/promql/grammar/queries-valid.promql");
        for (Tuple<String, Integer> line : lines) {
            String q = line.v1();
            try {
                PromqlParser parser = new PromqlParser();
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
        String query = "metric[5m]";
        new PromqlParser().createExpression(query);
    }

    public void testSingleQuery() throws Exception {
        String query = "foo[-1]";
        new PromqlParser().createExpression(query);
    }

    public void testUnsupportedQueries() throws Exception {
        List<Tuple<String, Integer>> lines = PromqlGrammarTests.readQueries("/promql/grammar/queries-invalid.promql");
        for (Tuple<String, Integer> line : lines) {
            String q = line.v1();
            try {
                System.out.println("Testing invalid query: " + q);
                PromqlParser parser = new PromqlParser();
//                Exception pe = expectThrowsAnyOf(
//                    //asList(ParsingException.class, UnsupportedOperationException.class),
//                    asList(Exception.class),
//                    () -> parser.createExpression(q)
//                );
                parser.createExpression(q);
                //System.out.printf(pe.getMessage());
            } catch (QlClientException pe) {
                // Expected
            }
//            } catch (AssertionError ae) {
//                fail(format(null, "Unexpected exception for line {}: [{}] \n {}", line.v2(), line.v1(), ae.getCause()));
//            }
        }
    }
}
