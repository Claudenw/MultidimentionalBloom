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
import org.apache.commons.collections4.bloomfilter.hasher.MD5;
import org.apache.commons.collections4.bloomfilter.hasher.Murmur128;
import org.apache.commons.collections4.bloomfilter.hasher.Murmur32;
import org.apache.commons.collections4.bloomfilter.hasher.ObjectsHash;

/**
 * The class that performs hashing on demand. Items can be added to the hasher using the
 * {@code with()} methods. once {@code getBits()} method is called it is an error to call
 * {@code with()} again.
 * @since 4.5
 */
public class CachingHasher implements Hasher {

    /**
     * The list of byte arrays that are to be hashed.
     */
    private final List<long[]> buffers;

    /**
     * The name of the hash function.
     */
    private final String name;

    /**
     * Constructs a DynamicHasher.
     *
     * @param name the name for the function.
     * @param function the function to use.
     * @param buffers the byte buffers that will be hashed.
     */
    public CachingHasher(String name, List<long[]> buffers) {
        this.buffers = new ArrayList<long[]>(buffers);
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Return an iterator of integers that are the bits to enable in the Bloom filter
     * based on the shape. The iterator may return the same value multiple times. There is
     * no guarantee made as to the order of the integers.
     *
     * @param shape the shape of the desired Bloom filter.
     * @return the Iterator of integers;
     * @throws IllegalArgumentException if {@code shape.getHasherName()} does not equal
     * {@code getName()}
     */
    @Override
    public PrimitiveIterator.OfInt getBits(Shape shape) {
        if (!getName().equals(shape.getHashFunctionName())) {
            throw new IllegalArgumentException(
                String.format("Shape hasher %s is not %s", shape.getHashFunctionName(), getName()));
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
     * A factory that produces DynamicHasher Builders.
     * @since 4.5
     */
    public static class Factory implements Hasher.Factory {

        /**
         * A map of functions names to functions.
         */
        private final Map<String, Constructor<? extends ToLongBiFunction<byte[], Integer>>> funcMap;

        /**
         * Constructs a factory with well known hash functions.
         */
        public Factory() {
            funcMap = new HashMap<String, Constructor<? extends ToLongBiFunction<byte[], Integer>>>();
            try {
                register(MD5.NAME, MD5.class);
            } catch (NoSuchMethodException | SecurityException e) {
                throw new IllegalStateException("Can not get MD5 constructor");
            }
            try {
                register(Murmur128.NAME, Murmur128.class);
            } catch (NoSuchMethodException | SecurityException e) {
                throw new IllegalStateException("Can not get Murmur128 constructor");
            }
        }

        /**
         * Registers a Hash function implementation. After registration the name can be
         * used to retrieve the Hasher. <p> The function calculates the long value that is
         * used to turn on a bit in the Bloom filter. The first argument is a
         * {@code byte[]} containing the bytes to be indexed, the second argument is a
         * seed index. </p><p> On the first call to {@code applyAsLong} the seed index
         * will be 0 and the function should start the hash sequence. </p> <p> On
         * subsequent calls the hash function using the same buffer the seed index will be
         * incremented. The function should return a different calculated value on each
         * call. The function may use the seed as part of the calculation or simply use it
         * to detect when the buffer has changed. </p>
         *
         * @see #useFunction(String)
         * @param name The name of the hash function
         * @param functionClass The function class for the hasher to use. Must have a zero
         * argument constructor.
         * @throws SecurityException if the no argument constructor can not be accessed.
         * @throws NoSuchMethodException if functionClass does not have a no argument
         * constructor.
         */
        protected void register(String name, Class<? extends ToLongBiFunction<byte[], Integer>> functionClass)
            throws NoSuchMethodException, SecurityException {
            Constructor<? extends ToLongBiFunction<byte[], Integer>> c = functionClass.getConstructor();
            funcMap.put(name, c);
        }

        @Override
        public Set<String> listFunctionNames() {
            return Collections.unmodifiableSet(funcMap.keySet());
        }

        @Override
        public CachingHasher.Builder useFunction(String name) {
            Constructor<? extends ToLongBiFunction<byte[], Integer>> c = funcMap.get(name);
            if (c == null) {
                throw new IllegalArgumentException("No function implementation named " + name);
            }
            try {
                return new Builder(name, c.newInstance());
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("Unable to call constructor for " + name, e);
            }
        }
    }

    /**
     * The builder for DyanamicHashers.
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
        private ToLongBiFunction<byte[], Integer> function;

        /**
         * The name for the function.
         */
        private String name;

        /**
         * Constructs a DynamicHasher builder.
         *
         * @param name the name of the function.
         * @param function the function implementation.
         */
        public Builder(String name, ToLongBiFunction<byte[], Integer> function) {
            this.name = name;
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
            for (byte[] buff : buffers)
            {
                long[] result = new long[2];
                result[0] = function.applyAsLong(buff, 0);
                result[1] = function.applyAsLong(buff, 1) - result[0];
                cache.add( result );
            }
            return new CachingHasher(name, cache);
        }

        @Override
        public final Builder with(byte property) {
            return with(new byte[] {property});
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
