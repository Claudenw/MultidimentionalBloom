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
package org.xenei.bloom.multidimensional.index.tri;

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
import java.util.stream.Collectors;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.xenei.bloom.filter.HasherCollection;
import org.xenei.bloom.multidimensional.Container.Index;

/**
 * An abstract Trie implementation.
 *
 * Bloom filters are chunked and each chunk is used to determine a leaf node
 * where the filter is stored. When searching each chunk is expanded into the
 * matching chunks as per the Bloom filter matching algorithm and all branches
 * explored.
 * <ul>
 * <li>m = number of bits in the bloom filter</li>
 * <li>N = number of unique filters stored in the trie.</li>
 * <li>c = chunk size
 * <li>Insert costs: O( m/c )</li>
 * <li>Search costs: O( 1.5^c *m/c )
 * <li>Memory requirements: O(2^c * m/c * N)</li>
 * </ul>
 * 
 * @param <I> The index type
 */
public abstract class Trie<I> implements Index<I> {

    /**
     * The shape of the contained Bloom filters.
     */
    protected final Shape shape;
    /**
     * A list of all leaf nodes created in the system
     */
    private final Map<I, LeafNode<I>> data;

    /**
     * The root node of the Trie.
     */
    private InnerNode<I> root;
    /**
     * The size of the chunks in the Trie in bits.
     */
    private final int chunkSize;
    /**
     * The mask for a single chunk.
     */
    private final long mask;
    /**
     * Function to convert Hasher to index.
     */
    private final Function<BitMapProducer, I> func;

    /**
     * Constructs a Trie.
     * 
     * @param estimatedPopulation the estimated number of Bloom filters to be
     * indexed.
     * @param shape the shape of the contained Bloom filters.
     * @param chunkSize the size of the Trie chunks in bits.
     * @param mask the mask to extract a single chunk.
     */
    protected Trie(Function<BitMapProducer, I> func, int estimatedPopulation, Shape shape, int chunkSize, long mask) {
        this.func = func;
        this.shape = shape;
        this.chunkSize = chunkSize;
        this.mask = mask;
        this.data = new HashMap<I, LeafNode<I>>(estimatedPopulation);
        root = new InnerNode<I>(0, this, null);
    }

    /**
     * Returns the matching nodes for a specific level in the Trie. The concrete
     * class will translate the chunk into all possible matching chunks. The
     * returned values must be unique but need not be ordered.
     * 
     * @param filter the filter to extract the matching nodes from.
     * @param level the level of the Trie that we are checking.
     * @return an array of all matching chunks.
     */
    protected abstract int[] getNodeIndexes(int chunk);

    /**
     * Get the chunk size for this Trie.
     * 
     * @return the chunk size for this trie.
     */
    public final int getChunkSize() {
        return chunkSize;
    }

    @Override
    public final I put(HasherCollection hashers) {
        return put(hashers.filterFor(shape));
    }

    public final I put(BloomFilter filter) {
        return put(filter.asBitMapArray());
    }

    public final I put(long[] bitMaps) {
        Optional<I> result = get(bitMaps);
        if (!result.isPresent()) {
            I idx = func.apply(BitMapProducer.fromBitMapArray(bitMaps));
            LeafNode<I> leafNode = root.add(idx, bitMaps);
            data.put(leafNode.getIdx(), leafNode);
            result = Optional.of(idx);
        }
        return result.get();
    }

    @Override
    public final void remove(I index) {
        LeafNode<I> leaf = data.get(index);
        if (leaf != null) {
            leaf.delete();
        }
    }

    @Override
    public final Set<I> search(HasherCollection hashers) {
        return search(hashers.filterFor(shape));
    }

    public final Set<I> search(BloomFilter filter) {
        return search(filter.asBitMapArray());
    }

    public final Set<I> search(long[] bitMaps) {
        Set<I> result = new HashSet<I>();
        root.search(result, bitMaps);
        return result;

    }

    @Override
    public Optional<I> get(HasherCollection hashers) {
        return get(hashers.filterFor(shape));
    }

    public Optional<I> get(BloomFilter filter) {
        return get(filter.asBitMapArray());
    }

    public Optional<I> get(long[] bitMaps) {
        List<LeafNode<I>> candidates = search(bitMaps).stream().map(data::get).filter(l -> l != null)
                .collect(Collectors.toList());
        I result = null;
        for (LeafNode<I> leaf : candidates) {
            List<InnerNode<I>> lst = new ArrayList<InnerNode<I>>();
            Node<I> n = leaf;
            while (n.getParent() != null) {
                lst.add(n.getParent());
                n = n.getParent();
            }
            Collections.reverse(lst);
            long[] values = assembleLongs(lst, leaf);
            if (Arrays.equals(values, bitMaps)) {
                if (result != null) {
                    throw new IllegalStateException("Too many results");
                }
                result = leaf.getIdx();
            }
        }

        return Optional.ofNullable(result);

    }

    /**
     * Get the array of long representation of the Bloom filter specified by the
     * leaf node. The Bloom filter is encoded into the Trie, this method extracts
     * it.
     * 
     * @param nodes the inner nodes that point to the leaf node in order from root
     * to inner node above leaf.
     * @param leaf the leaf node.
     * @return the long[] representation of the Bloom filter stored on the leaf.
     */
    private long[] assembleLongs(List<InnerNode<I>> nodes, LeafNode<I> leaf) {
        int limit = Double.valueOf(Math.ceil(shape.getNumberOfBits() / (double) Long.SIZE)).intValue();
        long[] result = new long[limit];

        for (int level = 0; level < nodes.size(); level++) {
            int longIdx = level * chunkSize / Long.SIZE;
            long val;
            if (level < nodes.size() - 1) {
                val = nodes.get(level).find(nodes.get(level + 1));
            } else {
                val = nodes.get(level).find(leaf);
            }
            if (val != 0) {
                result[longIdx] |= val << chunkSize * level;
            }
        }

        return result;
    }

    /**
     * Get the chunk for a specific level from a Bloom filter.
     * 
     * @param filter the Bloom filter to extract the chunk from.
     * @param level the level of the chunk.
     * @return the specified chunk.
     */
    public final int getChunk(long[] bitMaps, int level) {

        int chunkOffset = level * chunkSize;

        int idx = chunkOffset / Long.SIZE;
        if (idx >= bitMaps.length) {
            return 0x0;
        }
        int ofs = Math.floorMod(chunkOffset, Long.SIZE);

        return (int) ((bitMaps[idx] >> ofs) & mask);
    }

    @Override
    public Shape getShape() {
        return shape;
    }

    @Override
    public Set<I> getAll() {
        return new HashSet<I>(data.keySet());
    }

    public int getMaxDepth() {
        return (int) Math.ceil(shape.getNumberOfBits() * 1.0 / getChunkSize());
    }

    @Override
    public int getFilterCount() {
        return data.size();
    }

}
