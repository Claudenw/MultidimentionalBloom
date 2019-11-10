package org.xenei.bloom.multidimensional;

import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;

public class ContainerImpl<E> implements Container<E> {

    private Storage<E> storage;
    private Shape shape;
    private Index index;

    ContainerImpl(Shape shape, Storage<E> storage, Index index) {
        this.shape = shape;
        this.storage = storage;
        this.index = index;
    }

    @Override
    public Stream<E> get(BloomFilter filter) {
        verifyShape(filter);
        int idx = index.get(filter.getHasher());
        if (idx == -1) {
            return Container.emptyStream();
        }
        return storage.get(idx);
    }

    @Override
    public void put(BloomFilter filter, E value) {
        verifyShape(filter);
        Hasher hasher = filter.getHasher();
        int idx = index.get(hasher);
        if (idx == -1) {
            idx = index.put(hasher);
        }
        storage.put(idx, value);
    }

    @Override
    public void remove(BloomFilter filter, E value) {
        verifyShape(filter);
        Hasher hasher = filter.getHasher();
        int idx = index.get(hasher);
        if (idx != -1) {
            if (storage.remove(idx, value)) {
                index.remove(idx);
            }
        }
    }

    @Override
    public Stream<E> search(BloomFilter filter) {
        verifyShape(filter);
        Stream<E> result = Container.emptyStream();
        Iterator<Stream<E>> iter = index.search(filter.getHasher()).map(storage::get).iterator();
        while (iter.hasNext()) {
            result = Stream.concat(result, iter.next());
        }
        return result;
    }

    /**
     * Verify the other Bloom filter has the same shape as this Bloom filter.
     *
     * @param other the other filter to check.
     * @throws IllegalArgumentException if the shapes are not the same.
     */
    protected final void verifyShape(BloomFilter other) {
        verifyShape(other.getShape());
    }

    /**
     * Verify the specified shape has the same shape as this Bloom filter.
     *
     * @param shape the other shape to check.
     * @throws IllegalArgumentException if the shapes are not the same.
     */
    protected final void verifyShape(Shape shape) {
        if (!this.shape.equals(shape)) {
            throw new IllegalArgumentException(String.format("Shape %s is not the same as %s", shape, this.shape));
        }
    }

    /**
     * Verifies that the hasher has the same name as the shape.
     *
     * @param hasher the Hasher to check
     */
    protected final void verifyHasher(Hasher hasher) {
        if (!shape.getHashFunctionName().equals(hasher.getName())) {
            throw new IllegalArgumentException(
                    String.format("Hasher (%s) is not the hasher for shape (%s)", hasher.getName(), shape.toString()));
        }
    }

}
