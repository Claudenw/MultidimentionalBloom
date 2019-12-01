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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.xenei.bloom.multidimensional.Container.Storage;

public class IgniteStorage<E> implements Storage<E,UUID> {

    private static final String CACHE_NAME = "IgniteStorage";
    private Serde<E> serde;
    private ClientCache<UUID, List<byte[]>> cache;

    public IgniteStorage(IgniteClient igniteClient, Serde<E> serde) {
        this.serde = serde;
        this.cache = igniteClient.getOrCreateCache(CACHE_NAME);
    }

    @Override
    public Collection<E> get(UUID idx) {
        List<byte[]> cachedVal = cache.get(idx);
        if (cachedVal == null) {
            return Collections.emptyList();
        }
        return cachedVal.stream().map( serde::deserialize ).collect( Collectors.toList() );
    }

    @Override
    public void put(UUID idx, E value) {
        List<byte[]> cachedVal = cache.get(idx);
        if (cachedVal == null)
        {
            cachedVal = new ArrayList<byte[]>();
        }
        cachedVal.add( serde.serialize( value ));
        cache.getAndPut( idx,  cachedVal );
    }

    @Override
    public boolean[] remove(UUID idx, E value) {
        boolean[] result = new boolean[2];
        result[REMOVED] = false;
        result[EMPTY] = false;
        List<byte[]> lst = cache.get(idx);
        if (lst != null) {
            result[REMOVED] = lst.remove( serde.serialize(value));
            if (lst.isEmpty()) {
                cache.remove(idx);
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
    public Iterator<Map.Entry<UUID, List<E>>> list() {
        ScanQuery<UUID, List<byte[]>> scan = new ScanQuery<UUID, List<byte[]>>();
        QueryCursor<javax.cache.Cache.Entry<UUID, List<byte[]>>> cursor = cache.query(scan);
        return new TransformIterator<javax.cache.Cache.Entry<UUID, List<byte[]>>,Map.Entry<UUID, List<E>>>( cursor.iterator(), new Transformer
                <javax.cache.Cache.Entry<UUID, List<byte[]>>,
                Map.Entry<UUID, List<E>>>(){

            @Override
            public Entry<UUID, List<E>> transform(
                    javax.cache.Cache.Entry<UUID, List<byte[]>> input) {
                return new Converter(input);
            }});
    }


    /**
     * Converts a javax.cache.Cache.Entry<UUID, List<byte[]>> to a Map.Entry<UUID, List<E>>.
     *
     */
    class Converter implements Map.Entry<UUID, List<E>> {

        private javax.cache.Cache.Entry<UUID, List<byte[]>> ce;

        Converter( javax.cache.Cache.Entry<UUID, List<byte[]>> ce ) {
            this.ce = ce;
        }

        @Override
        public UUID getKey() {
            return ce.getKey();
        }

        @Override
        public List<E> getValue() {
            return ce.getValue().stream().map( serde::deserialize ).collect( Collectors.toList() );
        }

        @Override
        public List<E> setValue(List<E> arg0) {
            throw new UnsupportedOperationException();
        }

    }

}
