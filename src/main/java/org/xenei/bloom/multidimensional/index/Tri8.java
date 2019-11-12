package org.xenei.bloom.multidimensional.index;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.xenei.bloom.multidimensional.index.tri.Tri;

public class Tri8 extends  Tri {
    public static final int WIDTH = 8;

    private static final int[][] byteTable;

    static {
        int limit = (1 << Byte.SIZE + 1) - 1;
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
        super(shape, WIDTH );
    }

    @Override
    public byte getChunk(BloomFilter filter, int level) {
        long[] buffer = filter.getBits();

        int idx = level / Long.BYTES;
        if (idx >= buffer.length) {
            return 0x0;
        }
        int ofs = Math.floorMod(level, Long.BYTES);

        return (byte) ((buffer[idx] >> ofs) & 0xffL);
    }

    @Override
    public int[] getNodeIndexes(BloomFilter filter, int level) {
        return byteTable[getChunk(filter,level)];
    }




}
