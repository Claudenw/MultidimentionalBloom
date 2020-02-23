/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.bloom.multidimensional;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

import org.apache.commons.collections4.iterators.LazyIteratorChain;
import org.apache.commons.collections4.iterators.UnmodifiableIterator;
import org.xenei.bloom.filter.EWAHBloomFilter;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.hasher.HashFunctionIdentity;
import org.apache.commons.collections4.bloomfilter.hasher.HashFunctionValidator;
import org.apache.commons.collections4.bloomfilter.hasher.Hasher;
import org.apache.commons.collections4.bloomfilter.hasher.Shape;
import org.apache.commons.collections4.bloomfilter.CountingBloomFilter;

/**
 * An iplementation of a Multidimensional Bloom filter.
 *
 * @param <E> The type of object to be stored.
 * @param <I> The type of object used for the index.
 */
public class ContainerImpl<E,I> implements Container<E> {
    /**
     * The storage for the objects.
     */
    private Storage<E,I> storage;
    /**
     * The shape of the Blom filters in the container.
     */
    private Shape shape;
    /**
     * The index of the Bloom filters.
     */
    private Index<I> index;
    /**
     * The number of values in the container.
     */
    private int valueCount;

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
    public ContainerImpl(Shape shape, Storage<E,I> storage, Index<I> index) {
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
    public ContainerImpl(int estimatedPopulation, Shape shape, Storage<E,I> storage, Index<I> index) {
        this.shape = shape;
        this.storage = storage;
        this.index = index;
        this.valueCount = 0;
        Shape gateShape = new Shape(shape.getHashFunctionIdentity(), estimatedPopulation, shape.getProbability());
        gate = new CountingBloomFilter(gateShape);
    }

    @Override
    public int getValueCount() {
        return valueCount;
    }

    @Override
    public int getFilterCount() {
        return index.getFilterCount();
    }

    @Override
    public Shape getShape() {
        return shape;
    }

    @Override
    public Iterator<E> get(Hasher hasher) {
        verifyHasher(hasher);

        if (gate.contains(hasher)) {
            Optional<I> idx = index.get(hasher);
            if (idx.isPresent()) {
                return getEntryIterator(idx.get());
            }
        }
        return Collections.emptyIterator();
    }

    @Override
    public void put(Hasher hasher, E value) {
        verifyHasher(hasher);
        gate.merge(hasher);
        I idx = index.create( hasher );
        index.put( idx, hasher );
        storage.put( idx, value);
        valueCount++;
    }

    @Override
    public void remove(Hasher hasher, E value) {
        verifyHasher(hasher);

        if (gate.contains(hasher)) {
            Optional<I> idx = index.get(hasher);
            if (idx.isPresent()) {

                boolean[] result = storage.remove(idx.get(), value);
                if (result[Storage.REMOVED]) {
                    BloomFilter gateFilter = new EWAHBloomFilter(hasher, gate.getShape());
                    valueCount--;
                    gate.remove(gateFilter);
                    if (result[Storage.EMPTY]) {
                        index.remove(idx.get());
                    }
                }
            }
        }
    }

    @Override
    public Iterator<E> search(Hasher hasher) {
        verifyHasher(hasher);

        if (hasher.isEmpty())
        {
            Iterator<I> iter = index.getAll().iterator();
            // we are searching for all the items.
            return new LazyIteratorChain<E>() {
                @Override
                protected Iterator<E> nextIterator(int count) {
                    return iter.hasNext() ? getEntryIterator(iter.next()) : null;
                }
            };

        }
        if (gate.contains(hasher)) {
            Iterator<I> iter = index.search(hasher).iterator();
            return new LazyIteratorChain<E>() {
                @Override
                protected Iterator<E> nextIterator(int count) {
                    return iter.hasNext() ? getEntryIterator(iter.next()) : null;
                }
            };
        }
        return Collections.emptyListIterator();

    }

    private Iterator<E> getEntryIterator( I index ) {
        return UnmodifiableIterator.unmodifiableIterator(storage.get(index).iterator());
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
        HashFunctionValidator.checkAreEqual( shape.getHashFunctionIdentity(), hasher.getHashFunctionIdentity());
    }


}
