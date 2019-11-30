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
package org.xenei.bloom.multidimensional.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.xenei.bloom.filter.EWAHBloomFilter;
import org.xenei.bloom.multidimensional.Container.Index;

import com.googlecode.javaewah.datastructure.BitSet;

/**
 * This is what Daniel called Bloofi2. Basically, instead of using a tree
 * structure like Bloofi (see BloomFilterIndex), we "transpose" the BitSets.
 *
 * Originally from
 * https://github.com/lemire/bloofi/blob/master/src/mvm/provenance/FlatBloomFilterIndex.java
 *
 * @param <I> the index type.
 */
public final class RangePacked<I> implements Index<I> {

    /**
     * The shape of the bloom filters.
     */
    private final Shape shape;

    /**
     * A list of buffers. Each entry in the buffer is 64 filters. Each long in the
     * long[] is a bit in the bloom filter.
     */
    private BitSet[] buffer;

    /**
     * A bitset that indicates which entries are in use. entry/64 = buffer index.
     */
    private BitSet busy;

    /**
     * Function to convert BloomFilter to index.
     */
    private final Function<BloomFilter,I> func;

    /**
     * A list of index values matching the bitset values.
     */
    private final List<I> values;


    /**
     * Constructs a flat bloofi.
     *
     * @param shape the Shape of the contained Bloom filters.
     */
    public RangePacked(Function<BloomFilter,I> func, Shape shape) {
        this.func = func;
        this.shape = shape;
        this.buffer =new  BitSet[shape.getNumberOfBits()];
        this.busy = new BitSet(0);
        this.values = new ArrayList<I>();
    }

    /**
     * Clear (remove) the bloom filter from the buffers
     *
     * @param idx the index of the bloom filter in the busy bit set.
     */
    private void clearBloomAt(int idx) {
        busy.unset(idx);
        Arrays.stream(buffer).forEach(bs -> {if (bs != null && idx < bs.size()) bs.unset(idx);} );
    }

    private void setBuffer( int buffSize, int buffIdx, int bitIdx)
    {
        if (buffer[buffIdx] == null)
        {
            buffer[buffIdx] = new BitSet( buffSize );
        } else if (buffer[buffIdx].size()<buffSize) {
            buffer[buffIdx].resize( buffSize );
        }
        buffer[buffIdx].set( bitIdx );
    }
    /**
     * Using the hasher set the bits for a bloom filter.
     *
     * @param idx    the index of the bloom filter in the busy set.
     * @param hasher the hasher to generate the bits to turn on.
     */
    private void setBloomAt(int idx, Hasher hasher) {
        busy.set(idx);
        int buffSize = Long.SIZE *( 1 + (idx / Long.SIZE));
        hasher.getBits(shape).forEachRemaining((IntConsumer) buffIdx -> setBuffer( buffSize, buffIdx, idx ));
    }

    @Override
    public Optional<I> get(Hasher hasher) {
        Set<Integer> wanted = new HashSet<Integer>();
        hasher.getBits(shape).forEachRemaining( (Consumer<Integer>) wanted::add );
        BitSet answer = new BitSet(busy.size());
        answer.or(busy);
        for (int buffIdx=0;buffIdx<buffer.length;buffIdx++)
        {
            if (wanted.contains( buffIdx))
            {
                if (buffer[buffIdx] == null)
                {
                    return Optional.empty();
                } else {
                    answer.and(buffer[buffIdx]);
                }
            } else {
                if (buffer[buffIdx] != null)
                {
                    answer.andNot(buffer[buffIdx]);
                }
            }
        }
        if (answer.cardinality() > 1) {
            throw new IllegalStateException();
        }
        int result = answer.nextSetBit(0);
        if (result == -1)
        {
            return Optional.empty();
        }
        return Optional.of( values.get(result));
    }

    @Override
    public I put(Hasher hasher) {
        int idx = busy.nextUnsetBit(0);
        if (idx < 0) {
            // extend the busy
            idx = busy.size();
            busy.resize(idx + Long.SIZE);
        }
        setBloomAt(idx, hasher);
        while (values.size() < idx+1)
        {
            values.add( null );
        }
        I result = func.apply( new EWAHBloomFilter(hasher,shape) );
        values.set(idx, result);
        return result;
    }

    @Override
    public void remove(I index) {
        int idx = values.indexOf(index);
        if (idx > -1)
        {
            clearBloomAt(idx);
            values.set(idx, null);
        }
    }

    @Override
    public Set<I> search(Hasher hasher) {
        BitSet answer = new BitSet(busy.size());
        answer.or(busy);
        PrimitiveIterator.OfInt iter = hasher.getBits(shape);
        while (iter.hasNext())
        {
            int buffIdx = iter.next();
            if (buffer[buffIdx] == null)
            {
                return Collections.emptySet();
            }
            answer.and( buffer[buffIdx]);
        }
        Set<I> result = new HashSet<I>();
        answer.iterator().forEachRemaining( i -> result.add( values.get(i) ));
        return result;
    }

}
