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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.xenei.bloom.filter.HasherCollection;
import org.xenei.bloom.multidimensional.Container.Index;

/**
 * A linear implementation of an index.
 * <p>
 * This implementation is adequate for smallish populations. Large populations
 * with high collision rates will see significant performance degredation.
 * </p>
 * 
 * @param <I> The index type
 */
public class Linear<I> implements Index<I> {
    /**
     * The shape of the bloom filters.
     */
    private Shape shape;
    /**
     * The list of bloom filters.
     */
    private Map<I, BloomFilter> data;

    /**
     * Function to convert BloomFilter to index.
     */
    private final Function<BitMapProducer, I> func;

    /**
     * A comparator for bloom filters to determine if they are bit for bit
     * identical.
     */
    private static Comparator<BloomFilter> comp = new Comparator<BloomFilter>() {
        /**
         * Comparator for two Filters. Compares them based on byte values. Primarily
         * used to compare filters for equality.
         *
         * @param filter1 the first filter.
         * @param filter2 the second filter.
         * @return -1, 0 or 1 as per Comparator
         * @see Comparator
         */
        @Override
        public int compare(BloomFilter filter1, BloomFilter filter2) {
            if (filter1 == null) {
                return filter2 == null ? 0 : -1;
            }
            if (filter2 == null) {
                return 1;
            }
            long[] num1 = filter1.asBitMapArray();
            long[] num2 = filter2.asBitMapArray();
            int limit = Integer.min(num1.length, num2.length);
            for (int i = 0; i < limit; i++) {
                int result = Long.compare(num1[i], num2[i]);
                if (result != 0) {
                    return result;
                }
            }
            return Integer.compare(num1.length, num2.length);
        }
    };

    /**
     * Constructs the Linear index.
     * 
     * @param func The function to convert bloom filter to index object.
     * @param estimatedPopulation the estimated number of Bloom filters to index.
     * @param shape the shape of the bloom filters.
     */
    public Linear(Function<BitMapProducer, I> func, int estimatedPopulation, Shape shape) {
        this.shape = shape;
        this.func = func;
        data = new HashMap<I, BloomFilter>(estimatedPopulation);
    }

    @Override
    public Optional<I> get(HasherCollection hashers) {
        BloomFilter bf = hashers.filterFor(shape);
        return data.entrySet().stream().filter(entry -> {
            return comp.compare(entry.getValue(), bf) == 0;
        }).map(Map.Entry::getKey).findFirst();
    }

    @Override
    public I put(HasherCollection hashers) {
        BloomFilter filter = hashers.filterFor(shape);
        I result = func.apply(filter);
        data.put(result, filter);
        return result;
    }

    @Override
    public void remove(I index) {
        data.remove(index);
    }

    @Override
    public Set<I> search(HasherCollection hashers) {
        BloomFilter bf = hashers.filterFor(shape);
        return data.entrySet().stream().filter(entry -> {
            return entry.getValue().contains(bf);
        }).map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    @Override
    public int getFilterCount() {
        return data.size();
    }

    @Override
    public Shape getShape() {
        return shape;
    }

    @Override
    public Set<I> getAll() {
        return new HashSet<I>(data.keySet());
    }

}
