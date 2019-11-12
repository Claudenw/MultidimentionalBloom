package org.xenei.bloom.multidimensional.index;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.xenei.bloom.multidimensional.index.tri.InnerNode;
import org.xenei.bloom.multidimensional.index.tri.Tri;

public class Tri4 extends Tri {

    private static final int WIDTH = 4;

    private static final int[][] nibbleTable = { { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF },
            { 1, 3, 5, 7, 9, 0xB, 0xD, 0xF }, { 2, 3, 6, 7, 0xA, 0xB, 0xE, 0xF }, { 3, 7, 0xB, 0xF },
            { 4, 5, 6, 7, 0xC, 0xD, 0xE, 0xF }, { 5, 7, 0xD, 0xF }, { 6, 7, 0xE, 0xF }, { 7, 0xF },
            { 8, 9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF }, { 9, 0xB, 0xD, 0xF }, { 0xA, 0xB, 0xE, 0xF }, { 0xB, 0xF },
            { 0xC, 0xD, 0xE, 0xF }, { 0xD, 0xF }, { 0xE, 0xF }, { 0xF }, };

    private InnerNode root;

    public Tri4(Shape shape) {
        super(shape, WIDTH);
    }

    @Override
    public byte getChunk(BloomFilter filter, int level) {
        long[] buffer = filter.getBits();

        int idx = level / Long.BYTES;
        if (idx >= buffer.length) {
            return 0x0;
        }
        int ofs = Math.floorMod(level, Long.BYTES);

        int b = (byte) ((buffer[idx] >> ofs) & 0xffL);
        int nibble;
        if (level % 2 == 0) {
            nibble = 0x0F & (b >> 4);
        } else {
            nibble = 0x0F & b;
        }
        return (byte) nibble;
    }

    @Override
    public int[] getNodeIndexes(BloomFilter filter, int level) {
        return nibbleTable[getChunk(filter,level)];
    }


}
