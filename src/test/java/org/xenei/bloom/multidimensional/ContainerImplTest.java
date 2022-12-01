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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.LongPredicate;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.junit.Test;
import org.xenei.bloom.filter.HasherCollection;
import org.xenei.bloom.filter.HasherFactory;
import org.xenei.bloom.multidimensional.Container.Index;
import org.xenei.bloom.multidimensional.Container.Storage;
import org.xenei.bloom.multidimensional.index.FlatBloofi;
import org.xenei.bloom.multidimensional.storage.InMemory;

public class ContainerImplTest {
    Shape shape = org.apache.commons.collections4.bloomfilter.Shape.fromNP(3, 1.0 / 3000000);
    Func func = new Func(shape);
    Storage<String, UUID> storage = new InMemory<String, UUID>();
    Index<UUID> index = new FlatBloofi<UUID>(func, shape);
    Container<String> container = new ContainerImpl<String, UUID>(3000000, shape, storage, index);

    @Test
    public void roundTrip() {
        String test = "Hello World";
        HasherCollection hashers = add(test);
        List<String> lst = new ArrayList<String>();
        container.get(hashers).forEachRemaining(lst::add);
        assertEquals(1, lst.size());
        assertEquals(test, lst.get(0));
    }

    private HasherCollection add(String s) {
        HasherCollection hashers = new HasherCollection();
        hashers.add(HasherFactory.hasherFor(s));
        container.put(hashers, s);
        return hashers;
    }

    @Test
    public void getTest() {
        String test = "Hello World";
        HasherCollection hashers = add(test);
        add("Goodbye Cruel World");
        add("Now is the time for all good men to come to the aid of their country");

        List<String> lst = new ArrayList<String>();
        container.get(hashers).forEachRemaining(lst::add);
        assertEquals(1, lst.size());
        assertEquals(test, lst.get(0));
    }

    @Test
    public void searchTest() {

        String test = "Hello World";
        String test2 = "Spring has Sprung";
        String test3 = "GoodBye Cruel World";
        container.put(HasherFactory.collectionFor(test.split(" ")), test);
        container.put(HasherFactory.collectionFor(test2.split(" ")), test2);
        container.put(HasherFactory.collectionFor(test3.split(" ")), test3);

        HasherCollection searchHashers = HasherFactory.collectionFor("Just another dog".split(" "));
        List<String> lst = new ArrayList<String>();
        container.get(searchHashers).forEachRemaining(lst::add);
        assertEquals(0, lst.size());

        searchHashers = HasherFactory.collectionFor(new String[] { "World" });
        container.search(searchHashers).forEachRemaining(lst::add);
        assertEquals(2, lst.size());
        assertTrue(lst.contains(test));
        assertTrue(lst.contains(test3));
    }

    @Test
    public void removeTest() {
        String test = "Hello World";
        HasherCollection hashers = add(test);
        add("Goodbye Cruel World");
        add("Now is the time for all good men to come to the aid of their country");

        List<String> lst = new ArrayList<String>();
        container.get(hashers).forEachRemaining(lst::add);
        assertEquals(1, lst.size());
        assertEquals(test, lst.get(0));

        container.remove(hashers, "Hello World too");
        lst.clear();
        container.get(hashers).forEachRemaining(lst::add);
        assertEquals(1, lst.size());
        assertEquals(test, lst.get(0));

        container.remove(hashers, "Hello World");
        lst.clear();
        container.get(hashers).forEachRemaining(lst::add);
        assertEquals(0, lst.size());

    }

    /**
     * A standard Func to use in testing where UUID creation is desired.
     *
     */
    public static class Func implements Function<BitMapProducer, UUID> {
        private int numberOfBytes;

        public Func(Shape shape) {
            numberOfBytes = shape.getNumberOfBits() / Byte.SIZE + ((shape.getNumberOfBits() % Byte.SIZE) > 0 ? 1 : 0);
        }

        private byte[] getBytes(BitMapProducer bitMapProducer) {
            byte[] buffer = new byte[numberOfBytes];

            bitMapProducer.forEachBitMap(new LongPredicate() {
                int idx = 0;

                @Override
                public boolean test(long word) {
                    for (int longOfs = 0; longOfs < Long.BYTES; longOfs++) {
                        buffer[idx++] = (byte) ((word >> (Byte.SIZE * longOfs)) & 0xFFL);
                        if (idx == numberOfBytes) {
                            return true;
                        }
                    }
                    return true;
                }

            });
            return buffer;
        }

        @Override
        public UUID apply(BitMapProducer bitMapProducer) {
            return UUID.nameUUIDFromBytes(getBytes(bitMapProducer));
        }

    }
}
