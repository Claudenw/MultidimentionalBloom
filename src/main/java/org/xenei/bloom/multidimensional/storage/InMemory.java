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
package org.xenei.bloom.multidimensional.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.xenei.bloom.multidimensional.Container.Storage;

public class InMemory<E, I> implements Storage<E, I> {

    private HashMap<I, List<E>> storage = new HashMap<I, List<E>>();

    @Override
    public Collection<E> get(I idx) {
        Collection<E> result = storage.get(idx);
        return result == null ? Collections.emptyList() : result;
    }

    @Override
    public void put(I idx, E value) {
        List<E> lst = storage.get(idx);
        if (lst == null) {
            lst = new ArrayList<E>();
            storage.put(idx, lst);
        }
        lst.add(value);
    }

    @Override
    public boolean[] remove(I idx, E value) {
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
    public Iterator<Entry<I, List<E>>> list() {
        return storage.entrySet().iterator();
    }

}
