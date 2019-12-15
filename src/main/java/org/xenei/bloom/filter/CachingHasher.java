/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.bloom.filter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.ToLongBiFunction;

import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.hasher.HashFunction;
import org.apache.commons.collections4.bloomfilter.hasher.HashFunctionIdentity;
import org.apache.commons.collections4.bloomfilter.hasher.HashFunctionIdentity.ProcessType;
import org.apache.commons.collections4.bloomfilter.hasher.function.MD5Cyclic;
import org.apache.commons.collections4.bloomfilter.hasher.function.Murmur128x86Cyclic;

/**
 * The class that performs hashing on demand. Items can be added to the hasher
 * using the {@code with()} methods. once {@code getBits()} method is called it
 * is an error to call {@code with()} again. <p> This hasher can only produce
 * cyclic hash values. Any hash method may be passed to the constructor however,
 * the name must specify a Cyclic hash (i.e. the last character of the name must
 * be 'C'). </p>
 */
public class CachingHasher implements Hasher {

    /**
     * The list of byte arrays that are to be hashed.
     */
    private final List<long[]> buffers;

    /**
     * The hash function identity
     */
    private final HashFunctionIdentity functionIdentity;

    /**
     * Constructs a DynamicHasher.
     *
     * @param functionIdentity The identity of the function.
     * @param function         the function to use.
     * @param buffers          the byte buffers that will be hashed.
     * @throws IllegalArgumentException if the name does not indicate a cyclic
     *                                  hashing function.
     */
    public CachingHasher(HashFunctionIdentity functionIdentity, List<long[]> buffers) {
        this.functionIdentity = checkIdentity(functionIdentity);
        this.buffers = new ArrayList<long[]>(buffers);
    }

    /**
     * Constructs a DynamicHasher.
     *
     * @param functionIdentity The identity of the function.
     * @param function         the function to use.
     * @param buffers          the byte buffers that will be hashed.
     * @throws IllegalArgumentException if the name does not indicate a cyclic
     *                                  hashing function.
     */
    public CachingHasher(HashFunctionIdentity functionIdentity, long[][] buffers) {
        this.functionIdentity = checkIdentity(functionIdentity);
        this.buffers = Arrays.asList(buffers);
    }

    /**
     * Check that the name is valid for this hasher.
     *
     * @param functionIdentity the Function Identity to check.
     */
    private static HashFunctionIdentity checkIdentity(HashFunctionIdentity functionIdentity) {
        if (functionIdentity.getProcessType() != ProcessType.CYCLIC) {
            throw new IllegalArgumentException("Only cyclic hash functions may be used in a caching hasher");
        }
        return functionIdentity;
    }

    @Override
    public HashFunctionIdentity getHashFunctionIdentity() {
        return functionIdentity;
    }

    /**
     * Return an iterator of integers that are the bits to enable in the Bloom
     * filter based on the shape. The iterator may return the same value multiple
     * times. There is no guarantee made as to the order of the integers.
     *
     * @param shape the shape of the desired Bloom filter.
     * @return the Iterator of integers;
     * @throws IllegalArgumentException if {@code shape.getHasherName()} does not
     *                                  equal {@code getName()}
     */
    @Override
    public PrimitiveIterator.OfInt getBits(Shape shape) {
        if (HashFunctionIdentity.COMMON_COMPARATOR.compare(getHashFunctionIdentity(),
                shape.getHashFunctionIdentity()) != 0) {
            throw new IllegalArgumentException(String.format("Shape hasher %s is not %s",
                    HashFunctionIdentity.asCommonString(shape.getHashFunctionIdentity()),
                    HashFunctionIdentity.asCommonString(getHashFunctionIdentity())));
        }
        return new Iter(shape);
    }

    /**
     * The iterator of integers.
     */
    private class Iter implements PrimitiveIterator.OfInt {
        private int buffer = 0;
        private int funcCount = 0;
        private final Shape shape;
        private long accumulator;

        /**
         * Creates iterator with the specified shape.
         *
         * @param shape
         */
        private Iter(Shape shape) {
            this.shape = shape;
            this.accumulator = buffers.isEmpty() ? 0 : buffers.get(0)[0];
        }

        @Override
        public boolean hasNext() {
            if (buffers.isEmpty()) {
                return false;
            }
            return buffer < buffers.size() - 1 || funcCount < shape.getNumberOfHashFunctions();
        }

        @Override
        public int nextInt() {
            if (hasNext()) {
                if (funcCount >= shape.getNumberOfHashFunctions()) {
                    funcCount = 0;
                    buffer++;
                    accumulator = buffers.get(buffer)[0];
                }
                int result = (int) Math.floorMod(accumulator, (long) shape.getNumberOfBits());
                funcCount++;
                accumulator += buffers.get(buffer)[1];
                return result;
            }
            throw new NoSuchElementException();
        }
    }

    /**
     * The builder for DyanamicHashers.
     *
     * @since 4.5
     */
    public static class Builder implements Hasher.Builder {
        /**
         * The list of byte[] that are to be hashed.
         */
        private List<byte[]> buffers;

        /**
         * The function that the resulting DynamicHasher will use.
         */
        private HashFunction function;

        /**
         * Constructs a DynamicHasher builder.
         *
         * @param name     the name of the function.
         * @param function the function implementation.
         * @throws IllegalArgumentException if the name does not indicate a cyclic
         *                                  method.
         */
        public Builder(HashFunction function) {
            checkIdentity(function);
            this.function = function;
            this.buffers = new ArrayList<byte[]>();

        }

        /**
         * Builds the hasher.
         *
         * @return A DynamicHasher with the specified name, function and buffers.
         */
        @Override
        public CachingHasher build() throws IllegalArgumentException {
            List<long[]> cache = new ArrayList<long[]>();
            for (byte[] buff : buffers) {
                long[] result = new long[2];
                result[0] = function.apply(buff, 0);
                result[1] = function.apply(buff, 1) - result[0];
                cache.add(result);
            }
            return new CachingHasher(function, cache);
        }

        @Override
        public final Builder with(byte property) {
            return with(new byte[] { property });
        }

        @Override
        public final Builder with(byte[] property) {
            buffers.add(property);
            return this;
        }

        @Override
        public final Builder with(String property) {
            return with(property.getBytes(StandardCharsets.UTF_8));
        }

    }

}
