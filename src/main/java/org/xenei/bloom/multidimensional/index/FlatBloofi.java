package org.xenei.bloom.multidimensional.index;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Stream;

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
 * @author Daniel Lemire
 *
 * @param <E>
 */

public final class FlatBloofi implements Index {

    private final Shape shape;

    private ArrayList<long[]> buffer;

    private BitSet busy;

    public FlatBloofi(Shape shape) {
        this.shape = shape;
        this.buffer = new ArrayList<long[]>(0);
        this.busy = new BitSet(0);
    }

    private void clearBloomAt(int i) {
        final long[] mybuffer = buffer.get(i / 64);
        final long mask = ~(1l << i);
        for (int k = 0; k < mybuffer.length; ++k) {
            mybuffer[k] &= mask;
        }
    }

    private void setBloomAt(int i, Hasher hasher) {
        final long[] mybuffer = buffer.get(i / 64);
        final long mask = (1l << i);
        hasher.getBits(shape).forEachRemaining((IntConsumer) idx -> mybuffer[idx] |= mask);
    }

    @Override
    public int get(Hasher hasher) {

        BitSet answer = new BitSet(busy.length());
        answer.or(busy);
        Set<Integer> values = new HashSet<Integer>();
        hasher.getBits(shape).forEachRemaining((Consumer<Integer>) values::add);

        for (int bufferNumber = 0; bufferNumber < buffer.size(); ++bufferNumber) {
            if (answer.cardinality() == 0)
            {
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
                    if (answer.get(answIdx))
                    {
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
            idx = busy.length();
            busy.resize(idx + 64);
            //int longCount = Double.valueOf(Math.ceil(shape.getNumberOfBits() / (double) Long.SIZE)).intValue();
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
    public Stream<Integer> search(Hasher hasher) {
        List<Integer> answer = new ArrayList<Integer>();
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
        return answer.stream();
    }

}
