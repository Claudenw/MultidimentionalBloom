package org.xenei.bloom.multidimensional.index;

import java.util.Arrays;
import java.util.Collections;
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
public final class RangePacked implements Index {

    /**
     * The shape of the bloom filters.
     */
    private final Shape shape;

    /**
     * A list of buffers. Each entry in the buffer is 64 filters. Each long in the
     * long[] is a bit in the bloom filter.
     */
    private BitSet[] buffer;

    /**
     * A bitset that indicates which entries are in use. entry/64 = buffer index.
     */
    private BitSet busy;

    /**
     * Constructs a flat bloofi.
     *
     * @param shape the Shape of the contained Bloom filters.
     */
    public RangePacked(Shape shape) {
        this.shape = shape;
        this.buffer =new  BitSet[shape.getNumberOfBits()];
        this.busy = new BitSet(0);
    }

    /**
     * Clear (remove) the bloom filter from the buffers
     *
     * @param idx the index of the bloom filter in the busy bit set.
     */
    private void clearBloomAt(int idx) {
        busy.unset(idx);
        Arrays.stream(buffer).forEach(bs -> {if (bs != null && idx < bs.size()) bs.unset(idx);} );
    }

    private void setBuffer( int buffSize, int buffIdx, int bitIdx)
    {
        if (buffer[buffIdx] == null)
        {
            buffer[buffIdx] = new BitSet( buffSize );
        } else if (buffer[buffIdx].size()<buffSize) {
            buffer[buffIdx].resize( buffSize );
        }
        buffer[buffIdx].set( bitIdx );
    }
    /**
     * Using the hasher set the bits for a bloom filter.
     *
     * @param idx    the index of the bloom filter in the busy set.
     * @param hasher the hasher to generate the bits to turn on.
     */
    private void setBloomAt(int idx, Hasher hasher) {
        busy.set(idx);
        int buffSize = Long.SIZE *( 1 + (idx / Long.SIZE));
        hasher.getBits(shape).forEachRemaining((IntConsumer) buffIdx -> setBuffer( buffSize, buffIdx, idx ));
    }

    @Override
    public int get(Hasher hasher) {
        Set<Integer> wanted = new HashSet<Integer>();
        hasher.getBits(shape).forEachRemaining( (Consumer<Integer>) wanted::add );
        BitSet answer = new BitSet(busy.size());
        answer.or(busy);
        for (int buffIdx=0;buffIdx<buffer.length;buffIdx++)
        {
            if (wanted.contains( buffIdx))
            {
                if (buffer[buffIdx] == null)
                {
                    return NOT_FOUND;
                } else {
                    answer.and(buffer[buffIdx]);
                }
            } else {
                if (buffer[buffIdx] != null)
                {
                    answer.andNot(buffer[buffIdx]);
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
            busy.resize(idx + Long.SIZE);
        }
        setBloomAt(idx, hasher);
        return idx;
    }

    @Override
    public void remove(int index) {
        clearBloomAt(index);
    }

    @Override
    public Set<Integer> search(Hasher hasher) {
        BitSet answer = new BitSet(busy.size());
        answer.or(busy);
        PrimitiveIterator.OfInt iter = hasher.getBits(shape);
        while (iter.hasNext())
        {
            int buffIdx = iter.next();
            if (buffer[buffIdx] == null)
            {
                return Collections.emptySet();
            }
            answer.and( buffer[buffIdx]);
        }
        Set<Integer> result = new HashSet<Integer>();
        answer.iterator().forEachRemaining( result::add );
        return result;
    }

}
