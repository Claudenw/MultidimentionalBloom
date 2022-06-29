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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;

import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.HasherCollection;
import org.apache.commons.collections4.bloomfilter.SimpleHasher;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.junit.Test;
import org.xenei.bloom.multidimensional.Container.Index;
import org.xenei.bloom.multidimensional.Container.Storage;
import org.xenei.bloom.multidimensional.index.FlatBloofi;
import org.xenei.bloom.multidimensional.storage.InMemory;

public class ContainerImplTest {
    Shape shape = org.apache.commons.collections4.bloomfilter.Shape.fromNP( 3, 1.0 / 3000000);
    Func func = new Func(shape);
    Storage<String,UUID> storage = new InMemory<String,UUID>();
    Index<UUID> index = new FlatBloofi<UUID>(func,shape);
    Container<String> container = new ContainerImpl<String,UUID>(3000000, shape, storage, index);

    @Test
    public void roundTrip() {
        String test = "Hello World";
        long[] longs = MurmurHash3.hash128( test.getBytes( StandardCharsets.UTF_8 ));
        Hasher hasher = new SimpleHasher( longs[0], longs[1]);
        container.put(hasher, test);
        List<String> lst = new ArrayList<String>();
        container.get(hasher).forEachRemaining(lst::add);
        assertEquals(1, lst.size());
        assertEquals(test, lst.get(0));
    }

    private Hasher add(String s) {
        long[] longs = MurmurHash3.hash128( s.getBytes( StandardCharsets.UTF_8 ));
        Hasher hasher = new SimpleHasher( longs[0], longs[1]);
        container.put(hasher, s);
        return hasher;
    }

    @Test
    public void getTest() {
        String test = "Hello World";
        Hasher hasher = add(test);
        add("Goodbye Cruel World");
        add("Now is the time for all good men to come to the aid of their country");

        List<String> lst = new ArrayList<String>();
        container.get(hasher).forEachRemaining(lst::add);
        assertEquals(1, lst.size());
        assertEquals(test, lst.get(0));
    }

    private Hasher makeHasher(String s) {
        String[] parts = s.split(" ");
        HasherCollection hashers = new HasherCollection();
        //long[] longs = MurmurHash3.hash128( geoName.feature_code.getBytes( StandardCharsets.UTF_8 ));


        for (String part : parts) {
            long[] longs = MurmurHash3.hash128( part.getBytes( StandardCharsets.UTF_8 ));
            hashers.add( new SimpleHasher( longs[0], longs[1] ));
        }
        return hashers;

    }

    @Test
    public void searchTest() {

        String test = "Hello World";
        String test2 = "Spring has Sprung";
        String test3 = "GoodBye Cruel World";
        container.put(makeHasher(test), test);
        container.put(makeHasher(test2), test2);
        container.put(makeHasher(test3), test3);

        Hasher hasher = makeHasher("Just another dog");
        List<String> lst = new ArrayList<String>();
        container.get(hasher).forEachRemaining(lst::add);
        assertEquals(0, lst.size());

        hasher = makeHasher("World");
        container.search(hasher).forEachRemaining(lst::add);
        assertEquals(2, lst.size());
        assertTrue(lst.contains( test ));
        assertTrue(lst.contains(test3));
    }

    @Test
    public void removeTest() {
        String test = "Hello World";
        Hasher hasher = add(test);
        add("Goodbye Cruel World");
        add("Now is the time for all good men to come to the aid of their country");

        List<String> lst = new ArrayList<String>();
        container.get(hasher).forEachRemaining(lst::add);
        assertEquals(1, lst.size());
        assertEquals(test, lst.get(0));

        container.remove(hasher, "Hello World too");
        lst = new ArrayList<String>();
        container.get(hasher).forEachRemaining(lst::add);
        assertEquals(1, lst.size());
        assertEquals(test, lst.get(0));

        container.remove(hasher, "Hello World");
        lst = new ArrayList<String>();
        container.get(hasher).forEachRemaining(lst::add);
        assertEquals(0, lst.size());

    }

    /**
     * A standard Func to use in testing where UUID creation is desired.
     *
     */
    public static class Func implements Function<BitMapProducer,UUID> {
        private int numberOfBytes;

        public Func(Shape shape) {
            numberOfBytes = shape.getNumberOfBits() / Byte.SIZE + ((shape.getNumberOfBits() % Byte.SIZE) > 0?1:0);
        }

        private byte[] getBytes( BitMapProducer bitMapProducer)
        {
            byte[] buffer = new byte[numberOfBytes];

            bitMapProducer.forEachBitMap( new LongPredicate() {
            int idx = 0;
            @Override
            public boolean test(long word) {
                for (int longOfs=0;longOfs<Long.BYTES;longOfs++) {
                    buffer[idx++] = (byte) ((word>>(Byte.SIZE * longOfs))  & 0xFFL);
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
            return UUID.nameUUIDFromBytes(getBytes( bitMapProducer ));
        }

    }
}
