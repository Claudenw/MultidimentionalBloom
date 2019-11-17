package org.xenei.bloom.multidimensional.index;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.xenei.bloom.multidimensional.Container.Index;
import com.googlecode.javaewah.datastructure.BitSet;

/**
 * This is what Daniel called Bloofi2. Basically, instead of using a tree
 * structure like Bloofi (see BloomFilterIndex), we "transpose" the BitSets.
 *
 * Originally from
 * https://github.com/lemire/bloofi/blob/master/src/mvm/provenance/FlatBloomFilterIndex.java
 *
 * @param <E>
 */
public final class FlatBloofi implements Index {

    /**
     * The shape of the bloom filters.
     */
    private final Shape shape;

    /**
     * A list of buffers.
     * Each entry in the buffer is 64 filters.
     * Each long in the long[] is a bit in the bloom filter.
     */
    private ArrayList<long[]> buffer;

    /**
     * A bitset that indicates which entries are in use. entry/64 = buffer index.
     */
    private BitSet busy;

    /**
     * Constructs a flat bloofi.
     * @param shape the Shape of the contained Bloom filters.
     */
    public FlatBloofi(Shape shape) {
        this.shape = shape;
        this.buffer = new ArrayList<long[]>();
        this.busy = new BitSet(0);
    }

    /**
     * Clear (remove) the bloom filter from the buffers
     * @param idx the index of the bloom filter in the busy bit set.
     */
    private void clearBloomAt(int idx) {
        final long[] mybuffer = buffer.get(idx / 64);
        final long mask = ~(1l << idx);
        for (int k = 0; k < mybuffer.length; ++k) {
            mybuffer[k] &= mask;
        }
    }

    /**
     * Using the hasher set the bits for a bloom filter.
     * @param idx the index of the bloom filter in the busy set.
     * @param hasher the hasher to generate the bits to turn on.
     */
    private void setBloomAt(int idx, Hasher hasher) {
        final long[] mybuffer = buffer.get(idx / 64);
        final long mask = (1l << idx);
        hasher.getBits(shape).forEachRemaining((IntConsumer) i -> mybuffer[i] |= mask);
    }

    @Override
    public int get(Hasher hasher) {

        BitSet answer = new BitSet(busy.size());
        answer.or(busy);
        Set<Integer> values = new HashSet<Integer>();
        hasher.getBits(shape).forEachRemaining((Consumer<Integer>) values::add);

        for (int bufferNumber = 0; bufferNumber < buffer.size(); ++bufferNumber) {
            if (answer.cardinality() == 0) {
                return -1;
            }
            long[] buf = buffer.get(bufferNumber);
            for (int bitIdx = 0; bitIdx < buf.length; bitIdx++) {
                long l = buf[bitIdx];
                if (values.contains(bitIdx)) {
                    l &= 0xffffffffffffffffL;
                } else {
                    l ^= 0xffffffffffffffffL;
                }
                for (int bitNumber = 0; bitNumber < Long.SIZE; bitNumber++) {
                    int answIdx = bitNumber + (bufferNumber * Long.SIZE);
                    if (answer.get(answIdx)) {
                        answer.set(answIdx, (l & (1L << bitNumber)) > 0);
                    }
                }
            }
        }
        if (answer.cardinality() > 1) {
            throw new IllegalStateException();
        }
        return answer.nextSetBit(0);
    }

    @Override
    public int put(Hasher hasher) {
        int idx = busy.nextUnsetBit(0);
        if (idx < 0) {
            // extend the busy
            idx = busy.size();
            busy.resize(idx + 64);
            // int longCount = Double.valueOf(Math.ceil(shape.getNumberOfBits() / (double)
            // Long.SIZE)).intValue();
            buffer.add(new long[shape.getNumberOfBits()]);
        }
        setBloomAt(idx, hasher);
        busy.set(idx);
        return idx;
    }

    @Override
    public void remove(int index) {
        busy.unset(index);
        clearBloomAt(index);
    }

    @Override
    public Set<Integer> search(Hasher hasher) {
        Set<Integer> answer = new HashSet<Integer>();
        for (int i = 0; i < buffer.size(); ++i) {
            long w = ~0l;
            PrimitiveIterator.OfInt iter = hasher.getBits(shape);
            while (iter.hasNext()) {
                w &= buffer.get(i)[iter.nextInt()];
            }
            while (w != 0) {
                long t = w & -w;
                answer.add(i * Long.SIZE + Long.bitCount(t - 1));
                w ^= t;
            }
        }
        return answer;
    }

}
