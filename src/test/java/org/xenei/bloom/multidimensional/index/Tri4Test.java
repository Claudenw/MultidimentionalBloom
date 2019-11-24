package org.xenei.bloom.multidimensional.index;

import org.junit.runner.RunWith;
import org.xenei.junit.contract.Contract.Inject;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.junit.contract.IProducer;

@ContractImpl(Trie4.class)
@RunWith(ContractSuite.class)
public class Tri4Test {
    @Inject
    public IProducer<Trie4> getProducer() {
        return new IProducer<Trie4>() {

            @Override
            public Trie4 newInstance() {
                return new Trie4(IndexTest.SHAPE);
            }

            @Override
            public void cleanUp() {

            }
        };
    }

}
