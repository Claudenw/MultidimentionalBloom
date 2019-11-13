package org.xenei.bloom.multidimensional.index;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.xenei.bloom.multidimensional.index.tri.Tri;

public class Tri8 extends Tri {
    public static final int WIDTH = 8;
    public static final long MASK = 0xFFL;

    private static final int[][] byteTable;

    static {
        int limit = (1 << Byte.SIZE);
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

    public Tri8(Shape shape) {
        super(shape, WIDTH, MASK);
    }

    @Override
    public int[] getNodeIndexes(BloomFilter filter, int level) {
        return byteTable[getChunk(filter, level)];
    }

}
