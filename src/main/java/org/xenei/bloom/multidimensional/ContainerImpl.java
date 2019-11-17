package org.xenei.bloom.multidimensional;

import java.util.Collections;
import java.util.Iterator;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.iterators.LazyIteratorChain;
import org.xenei.bloom.filter.EWAHBloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.apache.commons.collections4.bloomfilter.CountingBloomFilter;

/**
 * An iplementation of a Multidimensional Bloom filter.
 *
 * @param <E> The type of object to be stored.
 */
public class ContainerImpl<E> implements Container<E> {
    /**
     * The storage for the objects.
     */
    private Storage<E> storage;
    /**
     * The shape of the Blom filters in the container.
     */
    private Shape shape;
    /**
     * The index of the Bloom filters.
     */
    private Index index;
    /**
     * The number of values in the container.
     */
    private int valueCount;
    /**
     * The number of filters in the container.
     */
    private int filterCount;
    /**
     * The Bloom filter that gates the container.
     */
    private CountingBloomFilter gate;

    /**
     * Constructs a Container.
     * Uses 1/shape.getProbability() as the estimated population.
     * @param shape the shape of the Bloom filter.
     * @param storage the storage for the objects
     * @param index the index for the bloom filter.
     */
    public ContainerImpl(Shape shape, Storage<E> storage, Index index) {
        this( Double.valueOf( 1/shape.getProbability()).intValue(), shape, storage, index);
    }

    /**
     * Constructs a Container.
     * <p>
     * This Container implementation is sensitive to the estimated population.  The
     * estimated population is used to create a Bloom filter that gates the index. If
     * the estimated population is too small additional overhead is consumed when putting
     * objects into the container as additional searches are performed.
     * </p>
     * @param estimatedPopulation the estimated number of objects in the container.
     * @param shape the shape of the Bloom filter.
     * @param storage the storage for the objects
     * @param index the index for the bloom filter.
     */
    public ContainerImpl(int estimatedPopulation, Shape shape, Storage<E> storage, Index index) {
        this.shape = shape;
        this.storage = storage;
        this.index = index;
        this.valueCount = 0;
        this.filterCount = 0;
        Shape gateShape = new Shape(shape.getHashFunctionName(), estimatedPopulation, shape.getProbability());
        gate = new CountingBloomFilter(gateShape);
    }

    @Override
    public int getValueCount() {
        return valueCount;
    }

    @Override
    public int getFilterCount() {
        return filterCount;
    }

    @Override
    public Shape getShape() {
        return shape;
    }

    @Override
    public Iterator<E> get(Hasher hasher) {
        verifyHasher(hasher);

        if (gate.contains(hasher)) {
            int idx = index.get(hasher);
            if (idx == -1) {
                return Collections.emptyIterator();
            }
            return storage.get(idx).iterator();
        }
        return Collections.emptyIterator();
    }

    @Override
    public void put(Hasher hasher, E value) {
        verifyHasher(hasher);
        boolean possibleDuplicate = gate.contains( hasher );
        gate.merge(hasher);
        int idx = possibleDuplicate? index.get(hasher) : -1;
        if (idx == -1) {
            idx = index.put(hasher);
            filterCount++;
        }
        storage.put(idx, value);
        valueCount++;
    }

    @Override
    public void remove(Hasher hasher, E value) {
        verifyHasher(hasher);

        if (gate.contains(hasher)) {
            int idx = index.get(hasher);
            if (idx != -1) {

                boolean[] result = storage.remove(idx, value);
                if (result[Storage.REMOVED]) {
                    BloomFilter gateFilter = new EWAHBloomFilter(hasher, gate.getShape());
                    valueCount--;
                    gate.remove(gateFilter);
                    if (result[Storage.EMPTY]) {
                        index.remove(idx);
                        filterCount--;
                    }
                }
            }
        }
    }

    @Override
    public Iterator<E> search(Hasher hasher) {
        verifyHasher(hasher);
        return doSearch(hasher);
    }

    /**
     * Performs a search using a hasher.
     * @param hasher the hasher to search with.
     * @return the stream of matchign objects.
     */
    private Iterator<E> doSearch(Hasher hasher) {
        if (gate.contains(hasher)) {
            Iterator<Integer> iter = index.search(hasher).iterator();
            return new LazyIteratorChain<E>() {
                     @Override
                    protected Iterator<E> nextIterator(int count) {
                         return iter.hasNext() ? storage.get(iter.next()).iterator() : null;
                    }
                 };
        }
        return Collections.emptyListIterator();

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
