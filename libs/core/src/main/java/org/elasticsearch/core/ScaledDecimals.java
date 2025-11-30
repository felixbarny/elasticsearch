/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2020 Elasticsearch B.V.
 */
package org.elasticsearch.core;

import java.util.Arrays;

/**
 * Decimal provides utilities for working with decimal numbers represented as v*10^e.
 * This is a port of the Go decimal package optimized for low allocations.
 * <p>
 * This is a Java port of the VictoriaMetrics decimal package.
 */
public class ScaledDecimals {

    static final long V_STALE_NAN = Long.MAX_VALUE - 1;
    static final long V_MAX = Long.MAX_VALUE - 2;
    static final long V_MIN = Long.MIN_VALUE + 1;

    // Stale NaN bits as used by Prometheus
    private static final long STALE_NAN_BITS = 0x7ff0000000000002L;

    public static final double STALE_NAN = Double.longBitsToDouble(STALE_NAN_BITS);

    private static final double CONVERSION_PRECISION = 1e12;

    private static final double LOG10_OF_2 = Math.log10(2);

    // Pre-calculated decimal multipliers
    private static final long[] DECIMAL_MULTIPLIERS = {
        1L,
        10L,
        100L,
        1000L,
        10000L,
        100000L,
        1000000L,
        10000000L,
        100000000L,
        1000000000L,
        10000000000L,
        100000000000L,
        1000000000000L,
        10000000000000L,
        100000000000000L,
        1000000000000000L,
        10000000000000000L,
        100000000000000000L,
        1000000000000000000L };
    /**
     * Thread-local buffer for temporary calculations to avoid allocations.
     */
    private static final ThreadLocal<ValuesAndExponentsBuffer> BUFFER_POOL = ThreadLocal.withInitial(ValuesAndExponentsBuffer::new);

    /**
     * Calibrates a and b with the corresponding exponents ae, be and returns the resulting exponent e.
     * Modifies a and b in place.
     *
     * @param a first array
     * @param ae exponent for a
     * @param b second array
     * @param be exponent for b
     * @return the calibrated exponent
     */
    public static short calibrateScale(long[] a, short ae, long[] b, short be) {
        if (ae == be) {
            // Fast path - exponents are equal
            return ae;
        }
        if (a.length == 0) {
            return be;
        }
        if (b.length == 0) {
            return ae;
        }

        // Ensure ae >= be by swapping if needed
        long[] tempA = a;
        long[] tempB = b;
        short tempAe = ae;
        short tempBe = be;

        if (ae < be) {
            tempA = b;
            tempB = a;
            tempAe = be;
            tempBe = ae;
        }

        short upExp = (short) (tempAe - tempBe);
        short downExp = 0;

        for (long v : tempA) {
            short maxUpExp = maxUpExponent(v);
            if (upExp - maxUpExp > downExp) {
                downExp = (short) (upExp - maxUpExp);
            }
        }
        upExp -= downExp;

        if (upExp > 0) {
            long m = getDecimalMultiplier(upExp);
            for (int i = 0; i < tempA.length; i++) {
                if (isSpecialValue(tempA[i]) == false) {
                    tempA[i] *= m;
                }
            }
        }

        if (downExp > 0) {
            if (downExp > 18) {
                for (int i = 0; i < tempB.length; i++) {
                    if (isSpecialValue(tempB[i]) == false) {
                        tempB[i] = 0;
                    }
                }
            } else {
                long m = getDecimalMultiplier(downExp);
                for (int i = 0; i < tempB.length; i++) {
                    if (isSpecialValue(tempB[i]) == false) {
                        tempB[i] /= m;
                    }
                }
            }
        }

        return (short) (tempBe + downExp);
    }

    private static long getDecimalMultiplier(int exp) {
        if (exp >= DECIMAL_MULTIPLIERS.length || exp < 0) {
            return 1;
        }
        return DECIMAL_MULTIPLIERS[exp];
    }

    /**
     * Converts each item in va to f=v*10^e and appends it to dst.
     * Returns the number of items written to dst (starting from offset dstOffset).
     *
     * @param dst destination array
     * @param dstOffset offset in dst to start writing
     * @param va source values
     * @param e exponent
     * @return number of items written
     */
    public static int appendDecimalToDouble(double[] dst, int dstOffset, long[] va, short e) {
        if (va.length == 0) {
            return 0;
        }

        // Check if all zeros
        if (isAllZeros(va)) {
            Arrays.fill(dst, dstOffset, dstOffset + va.length, 0.0);
            return va.length;
        }

        if (e == 0) {
            // Check if all ones
            if (isAllOnes(va)) {
                Arrays.fill(dst, dstOffset, dstOffset + va.length, 1.0);
                return va.length;
            }

            // Fast path for e=0
            for (int i = 0; i < va.length; i++) {
                long v = va[i];
                if (isSpecialValue(v) == false) {
                    dst[dstOffset + i] = (double) v;
                } else if (v == Long.MAX_VALUE) {
                    dst[dstOffset + i] = Double.POSITIVE_INFINITY;
                } else if (v == Long.MIN_VALUE) {
                    dst[dstOffset + i] = Double.NEGATIVE_INFINITY;
                } else {
                    dst[dstOffset + i] = STALE_NAN;
                }
            }
            return va.length;
        }

        // Increase conversion precision for negative exponents by dividing by e10
        if (e < 0) {
            double e10 = Math.pow(10, -e);
            for (int i = 0; i < va.length; i++) {
                long v = va[i];
                if (isSpecialValue(v) == false) {
                    dst[dstOffset + i] = v / e10;
                } else if (v == Long.MAX_VALUE) {
                    dst[dstOffset + i] = Double.POSITIVE_INFINITY;
                } else if (v == Long.MIN_VALUE) {
                    dst[dstOffset + i] = Double.NEGATIVE_INFINITY;
                } else {
                    dst[dstOffset + i] = STALE_NAN;
                }
            }
            return va.length;
        }

        double e10 = Math.pow(10, e);
        for (int i = 0; i < va.length; i++) {
            long v = va[i];
            if (isSpecialValue(v) == false) {
                dst[dstOffset + i] = v * e10;
            } else if (v == Long.MAX_VALUE) {
                dst[dstOffset + i] = Double.POSITIVE_INFINITY;
            } else if (v == Long.MIN_VALUE) {
                dst[dstOffset + i] = Double.NEGATIVE_INFINITY;
            } else {
                dst[dstOffset + i] = STALE_NAN;
            }
        }
        return va.length;
    }

    /**
     * Converts each item in src to v*10^e and writes to dst.
     *
     * @param dst       destination array
     * @param dstOffset offset in dst to start writing
     * @param src       source values
     * @return the common exponent e
     */
    public static short appendDoubleToDecimal(long[] dst, int dstOffset, double[] src) {
        if (src.length == 0) {
            return 0;
        }

        if (isAllZeros(src)) {
            Arrays.fill(dst, dstOffset, dstOffset + src.length, 0L);
            return 0;
        }

        if (isAllOnes(src)) {
            Arrays.fill(dst, dstOffset, dstOffset + src.length, 1L);
            return 0;
        }

        ValuesAndExponentsBuffer vae = BUFFER_POOL.get();
        vae.ensureCapacity(src.length);

        // Determine the minimum exponent across all src items
        short minExp = Short.MAX_VALUE;
        ScaledDecimal tmpResult = new ScaledDecimal();

        for (int i = 0; i < src.length; i++) {
            fromDouble(src[i], tmpResult);
            vae.values[i] = tmpResult.value;
            vae.exponents[i] = tmpResult.exponent;
            if (tmpResult.exponent < minExp && isSpecialValue(tmpResult.value) == false) {
                minExp = tmpResult.exponent;
            }
        }

        // Determine whether all src items may be upscaled to minExp
        short downExp = 0;
        for (int i = 0; i < src.length; i++) {
            long v = vae.values[i];
            short exp = vae.exponents[i];
            short upExp = (short) (exp - minExp);
            short maxUpExp = maxUpExponent(v);
            if (upExp - maxUpExp > downExp) {
                downExp = (short) (upExp - maxUpExp);
            }
        }
        minExp += downExp;

        // Scale each item in src to minExp and write to dst
        for (int i = 0; i < src.length; i++) {
            long v = vae.values[i];
            if (isSpecialValue(v)) {
                // No need to scale special values
                dst[dstOffset + i] = v;
                continue;
            }

            short exp = vae.exponents[i];
            short adjExp = (short) (exp - minExp);

            while (adjExp > 0) {
                v *= 10;
                adjExp--;
            }
            while (adjExp < 0) {
                v /= 10;
                adjExp++;
            }
            dst[dstOffset + i] = v;
        }

        return minExp;
    }

    private static short maxUpExponent(long v) {
        if (v == 0 || isSpecialValue(v)) {
            // Any exponent allowed for zeros and special values
            return 1024;
        }
        if (v < 0) {
            v = -v;
        }
        if (v < 0) {
            // Handle corner case for v = Long.MIN_VALUE
            return 0;
        }

        if (v <= Long.MAX_VALUE / 1000000000000000000L) return 18;
        if (v <= Long.MAX_VALUE / 100000000000000000L) return 17;
        if (v <= Long.MAX_VALUE / 10000000000000000L) return 16;
        if (v <= Long.MAX_VALUE / 1000000000000000L) return 15;
        if (v <= Long.MAX_VALUE / 100000000000000L) return 14;
        if (v <= Long.MAX_VALUE / 10000000000000L) return 13;
        if (v <= Long.MAX_VALUE / 1000000000000L) return 12;
        if (v <= Long.MAX_VALUE / 100000000000L) return 11;
        if (v <= Long.MAX_VALUE / 10000000000L) return 10;
        if (v <= Long.MAX_VALUE / 1000000000L) return 9;
        if (v <= Long.MAX_VALUE / 100000000L) return 8;
        if (v <= Long.MAX_VALUE / 10000000L) return 7;
        if (v <= Long.MAX_VALUE / 1000000L) return 6;
        if (v <= Long.MAX_VALUE / 100000L) return 5;
        if (v <= Long.MAX_VALUE / 10000L) return 4;
        if (v <= Long.MAX_VALUE / 1000L) return 3;
        if (v <= Long.MAX_VALUE / 100L) return 2;
        if (v <= Long.MAX_VALUE / 10L) return 1;
        return 0;
    }

    /**
     * Rounds f to the given number of decimal digits after the point.
     *
     * @param f value to round
     * @param digits number of decimal digits
     * @return rounded value
     */
    public static double roundToDecimalDigits(double f, int digits) {
        if (isStaleNaN(f) || Double.isNaN(f)) {
            return f;
        }
        if (digits <= -100 || digits >= 100) {
            return f;
        }
        double m = Math.pow(10, digits);
        return Math.round(f * m) / m;
    }

    /**
     * Rounds f to value with the given number of significant figures.
     *
     * @param f value to round
     * @param digits number of significant figures
     * @return rounded value
     */
    public static double roundToSignificantFigures(double f, int digits) {
        if (isStaleNaN(f)) {
            // Do not modify stale nan mark value
            return f;
        }
        if (digits <= 0 || digits >= 18) {
            return f;
        }
        if (Double.isNaN(f) || Double.isInfinite(f) || f == 0) {
            return f;
        }

        long n = (long) Math.pow(10, digits);
        boolean isNegative = f < 0;
        if (isNegative) {
            f = -f;
        }

        ScaledDecimal result = new ScaledDecimal();
        positiveDoubleToDecimal(f, result);
        long v = result.value;
        short e = result.exponent;

        if (v > V_MAX) {
            v = V_MAX;
        }

        long rem = 0;
        while (v > n) {
            rem = v % 10;
            v /= 10;
            e++;
        }
        if (rem >= 5) {
            v++;
        }
        if (isNegative) {
            v = -v;
        }
        return toDouble(v, e);
    }

    /**
     * Returns f=v*10^e.
     *
     * @param v value
     * @param e exponent
     * @return floating point representation
     */
    public static double toDouble(long v, short e) {
        if (isSpecialValue(v)) {
            if (v == Long.MAX_VALUE) {
                return Double.POSITIVE_INFINITY;
            }
            if (v == Long.MIN_VALUE) {
                return Double.NEGATIVE_INFINITY;
            }
            return STALE_NAN;
        }
        double f = (double) v;
        // Increase conversion precision for negative exponents by dividing by e10
        if (e < 0) {
            return f / Math.pow(10, -e);
        }
        return f * Math.pow(10, e);
    }

    /**
     * Returns true if f represents Prometheus staleness mark.
     *
     * @param f value to check
     * @return true if stale NaN
     */
    public static boolean isStaleNaN(double f) {
        return Double.doubleToRawLongBits(f) == STALE_NAN_BITS;
    }

    /**
     * Returns true if i represents Prometheus staleness mark.
     *
     * @param i value to check
     * @return true if stale NaN
     */
    public static boolean isStaleNaNInt64(long i) {
        return i == V_STALE_NAN;
    }

    /**
     * Converts f to v*10^e and stores the result in the provided DecimalResult object.
     * It tries minimizing v.
     *
     * @param f value to convert
     * @param result result object to store value and exponent (will be modified)
     */
    public static void fromDouble(double f, ScaledDecimal result) {
        if (f == 0) {
            result.set(0, (short) 0);
            return;
        }
        if (isStaleNaN(f)) {
            result.set(V_STALE_NAN, (short) 0);
            return;
        }
        if (Double.isInfinite(f)) {
            fromDoubleInf(f, result);
            return;
        }
        if (f > 0) {
            positiveDoubleToDecimal(f, result);
            if (result.value > V_MAX) {
                result.value = V_MAX;
            }
        } else {
            positiveDoubleToDecimal(-f, result);
            result.value = -result.value;
            if (result.value < V_MIN) {
                result.value = V_MIN;
            }
        }
    }

    private static void fromDoubleInf(double f, ScaledDecimal result) {
        if (Double.isInfinite(f) && f > 0) {
            result.set(Long.MAX_VALUE, (short) 0);
        } else {
            result.set(Long.MIN_VALUE, (short) 0);
        }
    }

    static void positiveDoubleToDecimal(double f, ScaledDecimal result) {
        // There is no need in checking for f == 0, since it should be already checked by the caller
        long u = (long) f;
        // Slow path for floating point numbers
        if ((double) u != f) {
            positiveDoubleToDecimalSlow(f, result);
            return;
        }
        // Fast path for integers
        if (u < (1L << 55) && u % 10 != 0) {
            result.set(u, (short) 0);
            return;
        }
        getDecimalAndScale(u, result);
    }

    private static void getDecimalAndScale(long u, ScaledDecimal result) {
        short scale = 0;

        // Remove trailing garbage bits
        while (u >= (1L << 55)) {
            u /= 10;
            scale++;
        }

        if (u % 10 != 0) {
            result.set(u, scale);
            return;
        }

        // Minimize v by converting trailing zeros to scale
        do {
            u /= 10;
            scale++;
        } while (u != 0 && u % 10 == 0);
        result.set(u, scale);
    }

    private static void positiveDoubleToDecimalSlow(double d, ScaledDecimal result) {
        short scale = 0;
        double prec = CONVERSION_PRECISION;

        if (d > 1e6 || d < 1e-6) {
            // Normalize d
            if (d > 1e6) {
                // Increase conversion precision for big numbers
                prec = 1e15;
            }

            int exp = Math.getExponent(d) + 1;
            // Bound the exponent
            if (exp < -1022) {
                exp = -1022;
            } else if (exp > 1023) {
                exp = 1023;
            }
            scale = (short) (exp * LOG10_OF_2);
            d *= Math.pow(10, -scale);
        }

        // Multiply d by 100 until the fractional part becomes too small
        while (d < prec) {
            double x = Math.floor(d);
            double frac = d - x;
            if (frac * prec < x) {
                d = x;
                break;
            }
            if ((1 - frac) * prec < x) {
                d = x + 1;
                break;
            }
            d *= 100;
            scale -= 2;
        }

        long u = (long) d;
        if (u % 10 != 0) {
            result.set(u, scale);
            return;
        }

        // Minimize u by converting trailing zero to scale
        u /= 10;
        scale++;
        result.set(u, scale);
    }

    private static boolean isSpecialValue(long v) {
        return v > V_MAX || v < V_MIN;
    }

    // Helper methods for checking all zeros/ones
    private static boolean isAllZeros(long[] a) {
        for (long v : a) {
            if (v != 0) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAllOnes(long[] a) {
        for (long v : a) {
            if (v != 1) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAllZeros(double[] a) {
        for (double v : a) {
            if (v != 0.0) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAllOnes(double[] a) {
        for (double v : a) {
            if (v != 1.0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Result class for methods that return (long, short) tuple.
     * Reusable to avoid allocations.
     */
    public static class ScaledDecimal {
        public long value;
        public short exponent;

        public ScaledDecimal() {}

        public void set(long value, short exponent) {
            this.value = value;
            this.exponent = exponent;
        }
    }

    private static class ValuesAndExponentsBuffer {
        long[] values;
        short[] exponents;

        ValuesAndExponentsBuffer() {
            this.values = new long[1024]; // Initial capacity
            this.exponents = new short[1024];
        }

        void ensureCapacity(int size) {
            if (values.length < size) {
                values = new long[size];
                exponents = new short[size];
            }
        }
    }
}
