/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public class ParsingUtils {
    // time units recognized by Prometheus
    private static final Map<String, Long> TIME_UNITS;

    static {
        // NB: using JDK TimeUnit or ChronoUnits turns out to be verbose
        // hence the basic approach used below
        // NB2: using LHM to preserve insertion order for consistent strings around keys
        Map<String, Long> map = new LinkedHashMap<>();
        map.put("y", 1000L * 60 * 60 * 24 * 365);
        map.put("w", 1000L * 60 * 60 * 24 * 7);
        map.put("d", 1000L * 60 * 60 * 24);
        map.put("h", 1000L * 60 * 60);
        map.put("m", 1000L * 60);
        map.put("s", 1000L);
        map.put("ms", 1L);
        TIME_UNITS = unmodifiableMap(map);
    }

    private ParsingUtils() {}

    public static TimeValue parseTimeValue(Source source, String string) {
        char[] chars = string.toCharArray();

        long millis = 0;

        String errorPrefix = "Invalid time duration [{}], ";
        int current;
        Tuple<String, Long> lastUnit = null;
        for (int i = 0; i < chars.length;) {
            current = i;
            // number - look for digits
            while (current < chars.length && Character.isDigit(chars[current])) {
                current++;
            }
            // at least one digit needs to be specified
            if (current == i) {
                throw new ParsingException(source, errorPrefix + "no number specified at index [{}]", string, current);
            }
            String token = new String(chars, i, current - i);
            int number;
            try {
                number = Integer.parseInt(token);
            } catch (NumberFormatException ex) {
                throw new ParsingException(source, errorPrefix + "invalid number [{}]", string, token);
            }
            i = current;
            // unit - look for letters
            while (current < chars.length && Character.isLetter(chars[current])) {
                current++;
            }
            // at least one letter needs to be specified
            if (current == i) {
                throw new ParsingException(source, errorPrefix + "no unit specified at index [{}]", string, current);
            }
            token = new String(chars, i, current - i);
            i = current;

            Long msMultiplier = TIME_UNITS.get(token);
            if (msMultiplier == null) {
                throw new ParsingException(
                    source,
                    errorPrefix + "unrecognized time unit [{}], must be one of {}",
                    string,
                    token,
                    TIME_UNITS.keySet()
                );
            }
            if (lastUnit != null) {
                if (lastUnit.v2() < msMultiplier) {
                    throw new ParsingException(
                        source,
                        errorPrefix + "units must be ordered from the longest to the shortest, found [{}] before [{}]",
                        string,
                        lastUnit.v1(),
                        token
                    );
                } else if (lastUnit.v2().equals(msMultiplier)) {
                    throw new ParsingException(
                        source,
                        errorPrefix + "a given unit must only appear once, found [{}] multiple times",
                        string,
                        token
                    );
                }
            }
            lastUnit = new Tuple<>(token, msMultiplier);

            millis += number * msMultiplier;
        }

        return new TimeValue(millis);
    }
}
