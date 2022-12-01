package org.xenei.bloom.multidimensional.index;

import java.util.Arrays;

import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.Shape;

/**
 * To be used for testing only. The fixed indices must fit within the filter
 * shape.
 */
class FixedHasher implements Hasher {
    private final int[] indices;
    private int[] distinct;

    FixedHasher(int... indices) {
        this.indices = indices.clone();
    }

    @Override
    public IndexProducer indices(Shape shape) {
        checkIndices(shape);
        return IndexProducer.fromIndexArray(indices);
    }

    @Override
    public IndexProducer uniqueIndices(Shape shape) {
        checkIndices(shape);
        int[] d = distinct;
        if (d == null) {
            distinct = d = Arrays.stream(indices).distinct().toArray();
        }
        return IndexProducer.fromIndexArray(d);
    }

    private void checkIndices(Shape shape) {
        // Check the hasher is OK for the shape
        int bits = shape.getNumberOfBits();
        for (int i : indices) {
            // Assertions.assertTrue(i < bits);
            if (!(i < bits)) {
                throw new RuntimeException("bad index");
            }
        }
    }
}