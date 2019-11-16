package org.xenei.bloom.multidimensional.index;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.xenei.bloom.multidimensional.index.tri.Trie;

/**
 * A Trie index that uses 8 bit bytes as chunks.
 *
 * @see Trie
 */
public class Trie8 extends Trie {
    /**
     * The size of the chunks.
     */
    public static final int CHUNK_SIZE = Byte.SIZE;

    /**
     * The mask for the chunks
     */
    public static final long MASK = 0xFFL;

    /**
     * A list of bytes to matching bytes in the bloom filter.
     */
    private static final int[][] byteTable;


    static {
        // populate the byteTable
        int limit = (1 << CHUNK_SIZE);
        byteTable = new int[limit][];
        List<Integer> lst = new ArrayList<Integer>();

        for (int i = 0; i < limit; i++) {
            for (int j = 0; j < limit; j++) {
                if ((j & i) == i) {
                    lst.add(j);
                }
            }
            byteTable[i] = lst.stream().mapToInt(Integer::intValue).toArray();
            lst.clear();
        }

    }

    /**
     * Constructs a Trie8
     * @param shape the shape of the contained Bloom filters.
     */
    public Trie8(Shape shape) {
        super(shape, CHUNK_SIZE, MASK);
    }

    @Override
    public int[] getNodeIndexes(int chunk) {
        return byteTable[chunk];
    }

}
