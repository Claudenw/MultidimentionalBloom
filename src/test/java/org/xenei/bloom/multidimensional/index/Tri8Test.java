package org.xenei.bloom.multidimensional.index;

import org.junit.runner.RunWith;
import org.xenei.junit.contract.Contract.Inject;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.junit.contract.IProducer;

@ContractImpl(Trie8.class)
@RunWith(ContractSuite.class)
public class Tri8Test {

    @Inject
    public IProducer<Trie8> getProducer() {
        return new IProducer<Trie8>() {

            @Override
            public Trie8 newInstance() {
                return new Trie8(IndexTest.SHAPE);
            }

            @Override
            public void cleanUp() {

            }
        };
    }

}
