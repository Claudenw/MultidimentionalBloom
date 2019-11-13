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

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.hasher.StaticHasher;

import com.googlecode.javaewah.EWAHCompressedBitmap;

/**
 * A bloom filter that uses EWAH compressed bitmaps to store enabled bits. This
 * filter is a good choice for large filters (high m value) with a relatively
 * low number of functions (k value).
 *
 */
public class EWAHBloomFilter extends BloomFilter {

    /**
     * The bitset that defines this BloomFilter.
     */
    private EWAHCompressedBitmap bitSet;

    /**
     * Constructs a filter from a hasher and shape.
     * 
     * @param hasher the hasher to use
     * @param shape  the shape.
     */
    public EWAHBloomFilter(Hasher hasher, Shape shape) {
        this(shape);
        verifyHasher(hasher);
        hasher.getBits(shape).forEachRemaining((IntConsumer) bitSet::set);
    }

    /**
     * Constructors an empty filter with the prescribed shape.
     *
     * @param shape The BloomFilter.Shape to define this BloomFilter.
     */
    public EWAHBloomFilter(Shape shape) {
        super(shape);
        this.bitSet = new EWAHCompressedBitmap();
    }

    @Override
    public StaticHasher getHasher() {
        return new StaticHasher(bitSet.iterator(), getShape());
    }

    @Override
    public long[] getBits() {
        BitSet bs = new BitSet();
        bitSet.forEach(bs::set);
        return bs.toLongArray();
    }

    @Override
    public void merge(BloomFilter other) {
        verifyShape(other);
        bitSet = bitSet.or(new EWAHCompressedBitmap(LongBuffer.wrap(other.getBits())));
    }

    @Override
    public void merge(Hasher hasher) {
        verifyHasher(hasher);
        hasher.getBits(getShape()).forEachRemaining((IntConsumer) bitSet::set);
    }

    @Override
    public boolean contains(Hasher hasher) {
        verifyHasher(hasher);
        OfInt iter = hasher.getBits(getShape());
        while (iter.hasNext()) {
            if (!bitSet.get(iter.nextInt())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hammingValue() {
        return bitSet.cardinality();
    }

    @Override
    public String toString() {
        return bitSet.toString();
    }

    /**
     * Merge an EWAHBloomFilter into this one. <p> This method takes advantage of
     * the internal structure of the EWAHBloomFilter. </p>
     *
     * @param other the other EWAHBloomFilter filter.
     */
    public void merge(EWAHBloomFilter other) {
        verifyShape(other);
        bitSet = bitSet.or(other.bitSet);
    }

    /**
     * Calculate the andCardinality with another EWAHBloomFilter. <p> This method
     * takes advantage of the internal structure of the EWAHBloomFilter. </p>
     *
     * @param other the other EWAHBloomFilter filter.
     * @see #andCardinality(BloomFilter)
     */
    public int andCardinality(EWAHBloomFilter other) {
        verifyShape(other);
        return bitSet.andCardinality(other.bitSet);
    }

    /**
     * Calculate the orCardinality with another EWAHBloomFilter. <p> This method
     * takes advantage of the internal structure of the EWAHBloomFilter. </p>
     *
     * @param other the other EWAHBloomFilter filter.
     * @see #orCardinality(BloomFilter)
     */
    public int orCardinality(EWAHBloomFilter other) {
        verifyShape(other);
        return bitSet.orCardinality(other.bitSet);
    }

    /**
     * Calculate the xorCardinality with another EWAHBloomFilter. <p> This method
     * takes advantage of the internal structure of the EWAHBloomFilter. </p>
     *
     * @param other the other EWAHBloomFilter filter.
     * @see #xorCardinality(BloomFilter)
     */
    public int xorCardinality(EWAHBloomFilter other) {
        verifyShape(other);
        return bitSet.xorCardinality(other.bitSet);
    }

}
