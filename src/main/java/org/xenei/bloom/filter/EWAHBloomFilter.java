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

import java.nio.LongBuffer;
import java.util.BitSet;
import java.util.PrimitiveIterator.OfInt;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.hasher.Hasher;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.exceptions.NoMatchException;

import com.googlecode.javaewah.ChunkIterator;
import com.googlecode.javaewah.EWAHCompressedBitmap;

/**
 * A bloom filter that uses EWAH compressed bitmaps to store enabled bits. This
 * filter is a good choice for large filters (high m value) with a relatively
 * low number of functions (k value).
 *
 */
public class EWAHBloomFilter implements BloomFilter {

    /**
     * The bitset that defines this BloomFilter.
     */
    private EWAHCompressedBitmap bitSet;

    /**
     * The shape of this filter
     */
    private final Shape shape;

    /**
     * Constructs a filter from a hasher and shape.
     *
     * @param hasher the hasher to use
     * @param shape  the shape.
     */
    public EWAHBloomFilter(Shape shape, Hasher hasher) {
        this(shape);
        hasher.indices(shape).forEachIndex((IntConsumer) bitSet::set);
    }

    /**
     * Constructors an empty filter with the prescribed shape.
     *
     * @param shape The BloomFilter.Shape to define this BloomFilter.
     */
    public EWAHBloomFilter(Shape shape) {
        this.shape = shape;
        this.bitSet = new EWAHCompressedBitmap();
    }

    @Override
    public int cardinality() {
        return bitSet.cardinality();
    }

    @Override
    public String toString() {
        return bitSet.toString();
    }

    @Override
    public void forEachIndex(IntConsumer consumer) {
        bitSet.iterator().forEachRemaining( x -> consumer.accept(x));
    }

    @Override
    public void forEachBitMap(LongConsumer consumer) {
        BitMapProducer.fromIndexProducer( this, shape).forEachBitMap(consumer);
    }

    @Override
    public boolean isSparse() {
        return true;
    }

    @Override
    public Shape getShape() {
        return shape;
    }

    @Override
    public boolean contains(IndexProducer indexProducer) {
        EWAHCompressedBitmap producerBitSet = new EWAHCompressedBitmap();
        indexProducer.forEachIndex((IntConsumer) producerBitSet::set);
        return bitSet.andCardinality(producerBitSet) == producerBitSet.cardinality();
    }

    @Override
    public boolean contains(BitMapProducer bitMapProducer) {
        return contains( IndexProducer.fromBitMapProducer(bitMapProducer));
    }

    @Override
    public boolean mergeInPlace(BloomFilter other) {
        if (other instanceof EWAHBloomFilter) {
            bitSet = bitSet.or(((EWAHBloomFilter)other).bitSet);
        } else {
            other.forEachIndex( bitSet::set );
        }
        return true;
    }

}
