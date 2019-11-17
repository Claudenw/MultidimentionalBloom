package org.xenei.bloom.multidimensional.storage;

public interface Serde<E> {

    byte[] serialize( E data );
    E deserialize( byte[] data );

}
