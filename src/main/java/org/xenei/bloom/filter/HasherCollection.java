package org.xenei.bloom.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.Shape;

public class HasherCollection implements Collection<Hasher> {
    private final List<Hasher> delegate;

    public HasherCollection() {
        delegate = new ArrayList<>();
    }

    public HasherCollection(Hasher hasher) {
        delegate = new ArrayList<>();
        delegate.add(hasher);
    }

    public BloomFilter filterFor(Shape shape) {
        BloomFilter filter = new EWAHBloomFilter(shape);
        for (Hasher hasher : delegate) {
            filter.merge(hasher);
        }
        return filter;
    }

    public BloomFilter fill(BloomFilter filter) {
        for (Hasher hasher : delegate) {
            filter.merge(hasher);
        }
        return filter;
    }

    @Override
    public boolean add(Hasher arg0) {
        return delegate.add(arg0);
    }

    public void add(int arg0, Hasher arg1) {
        delegate.add(arg0, arg1);
    }

    @Override
    public boolean addAll(Collection<? extends Hasher> arg0) {
        return delegate.addAll(arg0);
    }

    public boolean addAll(int arg0, Collection<? extends Hasher> arg1) {
        return delegate.addAll(arg0, arg1);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public boolean contains(Object arg0) {
        return delegate.contains(arg0);
    }

    @Override
    public boolean containsAll(Collection<?> arg0) {
        return delegate.containsAll(arg0);
    }

    @Override
    public boolean equals(Object arg0) {
        return delegate.equals(arg0);
    }

    @Override
    public void forEach(Consumer<? super Hasher> arg0) {
        delegate.forEach(arg0);
    }

    public Hasher get(int arg0) {
        return delegate.get(arg0);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    public int indexOf(Object arg0) {
        return delegate.indexOf(arg0);
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public Iterator<Hasher> iterator() {
        return delegate.iterator();
    }

    public int lastIndexOf(Object arg0) {
        return delegate.lastIndexOf(arg0);
    }

    public ListIterator<Hasher> listIterator() {
        return delegate.listIterator();
    }

    public ListIterator<Hasher> listIterator(int arg0) {
        return delegate.listIterator(arg0);
    }

    @Override
    public Stream<Hasher> parallelStream() {
        return delegate.parallelStream();
    }

    public Hasher remove(int arg0) {
        return delegate.remove(arg0);
    }

    @Override
    public boolean remove(Object arg0) {
        return delegate.remove(arg0);
    }

    @Override
    public boolean removeAll(Collection<?> arg0) {
        return delegate.removeAll(arg0);
    }

    @Override
    public boolean removeIf(Predicate<? super Hasher> filter) {
        return delegate.removeIf(filter);
    }

    public void replaceAll(UnaryOperator<Hasher> operator) {
        delegate.replaceAll(operator);
    }

    @Override
    public boolean retainAll(Collection<?> arg0) {
        return delegate.retainAll(arg0);
    }

    public Hasher set(int arg0, Hasher arg1) {
        return delegate.set(arg0, arg1);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    public void sort(Comparator<? super Hasher> arg0) {
        delegate.sort(arg0);
    }

    @Override
    public Spliterator<Hasher> spliterator() {
        return delegate.spliterator();
    }

    @Override
    public Stream<Hasher> stream() {
        return delegate.stream();
    }

    public List<Hasher> subList(int arg0, int arg1) {
        return delegate.subList(arg0, arg1);
    }

    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public <T> T[] toArray(IntFunction<T[]> generator) {
        return delegate.toArray(generator);
    }

    @Override
    public <T> T[] toArray(T[] arg0) {
        return delegate.toArray(arg0);
    }

}
