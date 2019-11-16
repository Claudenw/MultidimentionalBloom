package org.xenei.bloom.multidimensional.index;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.xenei.bloom.multidimensional.index.tri.Trie;

/**
 * A Trie index that uses 4 bit nibbles as chunks.
 *
 * @see Trie
 */
public class Trie4 extends Trie {

    /**
     * The chunk size.
     */
    private static final int CHUNK_SIZE = 4;

    /**
     * the bit mask.
     */
    private static final long MASK = 0x0fL;

    /**
     * A list of nibbles to matching nibbles in the bloom filter.
     */
    private static final int[][] nibbleTable = { { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF },
            { 1, 3, 5, 7, 9, 0xB, 0xD, 0xF }, { 2, 3, 6, 7, 0xA, 0xB, 0xE, 0xF }, { 3, 7, 0xB, 0xF },
            { 4, 5, 6, 7, 0xC, 0xD, 0xE, 0xF }, { 5, 7, 0xD, 0xF }, { 6, 7, 0xE, 0xF }, { 7, 0xF },
            { 8, 9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF }, { 9, 0xB, 0xD, 0xF }, { 0xA, 0xB, 0xE, 0xF }, { 0xB, 0xF },
            { 0xC, 0xD, 0xE, 0xF }, { 0xD, 0xF }, { 0xE, 0xF }, { 0xF }, };

    /**
     * Constructs a Trie4.
     * @param shape the shape of the contained Bloom filters.
     */
    public Trie4(Shape shape) {
        super(shape, CHUNK_SIZE, MASK);
    }

    @Override
    public int[] getNodeIndexes(int chunk) {
        return nibbleTable[chunk];
    }

}
