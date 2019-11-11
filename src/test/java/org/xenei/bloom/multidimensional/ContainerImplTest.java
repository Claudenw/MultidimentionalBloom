package org.xenei.bloom.multidimensional;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.Hasher.Factory;
import org.apache.commons.collections4.bloomfilter.hasher.Murmur128;
import org.junit.Test;
import org.xenei.bloom.filter.CachingHasher;
import org.xenei.bloom.multidimensional.Container.Index;
import org.xenei.bloom.multidimensional.Container.Storage;
import org.xenei.bloom.multidimensional.index.FlatBloofi;
import org.xenei.bloom.multidimensional.storage.InMemory;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;

public class ContainerImplTest {

    Shape shape = new Shape( Murmur128.NAME, 3, 1.0/3000000);
    Storage<String> storage = new InMemory<String>();
    Index index = new FlatBloofi( shape );
    Container<String> container = new ContainerImpl<String>( shape, storage, index );

    @Test
    public void ShapeValues() {
        System.out.println( shape.toString() );
        System.out.println( shape.getProbability());
        System.out.println( 1/shape.getProbability());
    }

    @Test
    public void roundTrip()
    {
        String test = "Hello World";
        Hasher hasher = Factory.DEFAULT.useFunction( Murmur128.NAME ).with( test ).build();
        container.put(hasher, test );
        List<String> lst = new ArrayList<String>();
        container.get(hasher).forEach( lst::add );
        assertEquals( 1, lst.size() );
        assertEquals( test, lst.get(0));
    }

    private Hasher add(String s)
    {
        Hasher hasher = Factory.DEFAULT.useFunction( Murmur128.NAME ).with( s ).build();
        container.put(hasher, s );
        return hasher;
    }

    @Test
    public void getTest()
    {
        String test = "Hello World";
        Hasher hasher = add( test );
        add( "Goodbye Cruel World");
        add( "Now is the time for all good men to come to the aid of their country");

        List<String> lst = new ArrayList<String>();
        container.get(hasher).forEach( lst::add );
        assertEquals( 1, lst.size() );
        assertEquals( test, lst.get(0));
    }

    @Test
    public void searchTest()
    {
        String test = "Hello World";
        String test2 = "Now is the time for all good men to come to the aid of their country";
        add( test );
        add("Goodbye Cruel World");
        add( test2 );

        // 29 and 63 are shared by both Hello World and Now is the time ...
        long[][] values = { {29, 0}, {63,0} };
        CachingHasher hasher = new CachingHasher( shape.getHashFunctionName(), Arrays.asList( values ));
        List<String> lst = new ArrayList<String>();
        container.get(  hasher ).forEach( lst::add );
        assertEquals( 0, lst.size() );
        container.search(  hasher ).forEach( lst::add );
        assertEquals( 2, lst.size() );
        assertEquals( test, lst.get(0));
        assertEquals( test2, lst.get(1));
    }

    @Test
    public void removeTest()
    {
        String test = "Hello World";
        Hasher hasher = add( test );
        add( "Goodbye Cruel World");
        add( "Now is the time for all good men to come to the aid of their country");

        List<String> lst = new ArrayList<String>();
        container.get(hasher).forEach( lst::add );
        assertEquals( 1, lst.size() );
        assertEquals( test, lst.get(0));

        container.remove( hasher, "Hello World too");
        lst = new ArrayList<String>();
        container.get(hasher).forEach( lst::add );
        assertEquals( 1, lst.size() );
        assertEquals( test, lst.get(0));

        container.remove( hasher, "Hello World");
        lst = new ArrayList<String>();
        container.get(hasher).forEach( lst::add );
        assertEquals( 0, lst.size() );

    }


}
