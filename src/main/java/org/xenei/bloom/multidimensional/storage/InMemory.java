package org.xenei.bloom.multidimensional.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.xenei.bloom.multidimensional.Container;
import org.xenei.bloom.multidimensional.Container.Storage;

public class InMemory<E> implements Storage<E> {

    private HashMap<Integer, List<E>> storage = new HashMap<Integer, List<E>>();

    @Override
    public Stream<E> get(int idx) {
        if (idx < 0 || storage.get(idx) == null) {
            return Container.emptyStream();
        }
        return storage.get(idx).stream();
    }

    @Override
    public void put(int idx, E value) {
        List<E> lst = storage.get(idx);
        if (lst == null) {
            lst = new ArrayList<E>();
            storage.put(idx, lst);
        }
        lst.add(value);

    }

    @Override
    public boolean[] remove(int idx, E value) {
        boolean[] result = new boolean[2];
        result[REMOVED] = false;
        result[EMPTY] = false;
        List<E> lst = storage.get(idx);
        if (lst != null) {
            result[REMOVED] = lst.remove(value);
            if (lst.isEmpty()) {
                storage.remove(idx);
                result[EMPTY] = true;
            } else {
                result[EMPTY] = false;
            }
        } else {
            result[EMPTY] = true;
        }
        return result;
    }

    @Override
    public Stream<Entry<Integer, List<E>>> list() {
        return storage.entrySet().stream();
    }

}
