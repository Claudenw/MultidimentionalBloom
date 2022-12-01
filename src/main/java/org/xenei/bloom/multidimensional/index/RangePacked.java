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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.xenei.bloom.filter.HasherCollection;
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
     * A list of buffers, one for each bit in the filter. Each entry in the buffer
     * is a bitset comprising the integer indexes for each filter that has that
     * particular bit enabled.
     */
    private BitSet[] buffer;

    /**
     * A bitset that indicates which entries are in use. entry/64 = buffer index.
     */
    private BitSet busy;

    /**
     * Function to convert BloomFilter to index.
     */
    private final Function<BitMapProducer, I> func;

    /**
     * A list of index values matching the bitset values.
     */
    private final List<I> values;

    /**
     * A map of values to internal indexes
     */
    private final Map<I, Integer> valueToIdx;

    /**
     * Constructs a flat bloofi.
     *
     * @param shape the Shape of the contained Bloom filters.
     */
    public RangePacked(Function<BitMapProducer, I> func, Shape shape) {
        this.func = func;
        this.shape = shape;
        this.buffer = new BitSet[shape.getNumberOfBits()];
        this.busy = new BitSet(0);
        this.values = new ArrayList<I>();
        this.valueToIdx = new HashMap<I, Integer>();
    }

    /**
     * Clear (remove) the bloom filter from the buffers
     *
     * @param idx the index of the bloom filter in the busy bit set.
     */
    private void clearBloomAt(int idx) {
        busy.unset(idx);
        Arrays.stream(buffer).forEach(bs -> {
            if (bs != null && idx < bs.size())
                bs.unset(idx);
        });
    }

    private boolean setBuffer(int buffSize, int buffIdx, int bitIdx) {
        if (buffer[buffIdx] == null) {
            buffer[buffIdx] = new BitSet(buffSize);
        } else if (buffer[buffIdx].size() < buffSize) {
            buffer[buffIdx].resize(buffSize);
        }
        buffer[buffIdx].set(bitIdx);
        return true;
    }

    /**
     * Using the hasher set the bits for a bloom filter.
     *
     * @param idx the index of the bloom filter in the busy set.
     * @param filter the filter that contains the bits to turn on.
     */
    private void setBloomAt(int idx, BloomFilter filter) {
        busy.set(idx);
        int buffSize = Long.SIZE * (1 + (idx / Long.SIZE));
        filter.forEachIndex(buffIdx -> setBuffer(buffSize, buffIdx, idx));
    }

    @Override
    public Optional<I> get(HasherCollection hashers) {
        return get(hashers.filterFor(shape));
    }

    private Optional<I> get(BloomFilter filter) {
        I result = func.apply(filter);
        return values.indexOf(result) == -1 ? Optional.empty() : Optional.of(result);
    }

    @Override
    public I put(HasherCollection hashers) {
        BloomFilter filter = hashers.filterFor(shape);
        Optional<I> result = get(filter);
        if (!result.isPresent()) {
            I idx = func.apply(filter);
            Integer i = valueToIdx.get(idx);
            if (i == null) {
                int index = busy.nextUnsetBit(0);
                if (index < 0) {
                    // extend the busy
                    index = busy.size();
                    busy.resize(index + Long.SIZE);
                }
                setBloomAt(index, filter);
                register(idx, index);
            }
            result = Optional.of(idx);
        }
        return result.get();
    }

    private void register(I value, int idx) {
        while (values.size() < busy.size()) {
            values.add(null);
        }
        values.set(idx, value);
        valueToIdx.put(value, idx);
    }

    @Override
    public void remove(I index) {
        int idx = values.indexOf(index);
        if (idx > -1) {
            clearBloomAt(idx);
            values.set(idx, null);
        }
        valueToIdx.remove(index);
    }

    @Override
    public Set<I> search(HasherCollection hashers) {
        BitSet answer = new BitSet(busy.size());
        answer.or(busy);

        BloomFilter filter = hashers.filterFor(shape);
        if (!filter.forEachIndex(buffIdx -> {
            if (buffer[buffIdx] == null) {
                return false;
            }
            answer.and(buffer[buffIdx]);
            return true;
        })) {
            return Collections.emptySet();
        }
        Set<I> result = new HashSet<I>();
        answer.iterator().forEachRemaining(i -> result.add(values.get(i)));
        return result;
    }

    @Override
    public int getFilterCount() {
        return busy.cardinality();
    }

    @Override
    public Shape getShape() {
        return shape;
    }

    @Override
    public Set<I> getAll() {
        return new HashSet<I>(values);
    }
}
