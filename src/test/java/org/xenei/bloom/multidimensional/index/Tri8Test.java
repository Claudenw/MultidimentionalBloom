package org.xenei.bloom.multidimensional.index;

import org.junit.runner.RunWith;
import org.xenei.junit.contract.Contract.Inject;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.junit.contract.IProducer;

@ContractImpl(Tri8.class)
@RunWith(ContractSuite.class)
public class Tri8Test {

    @Inject
    public IProducer<Tri8> getProducer() {
        return new IProducer<Tri8>() {

            @Override
            public Tri8 newInstance() {
                return new Tri8(IndexTest.shape);
            }

            @Override
            public void cleanUp() {

            }
        };
    }

}
