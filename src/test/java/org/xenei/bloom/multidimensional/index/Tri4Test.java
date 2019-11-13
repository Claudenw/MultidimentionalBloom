package org.xenei.bloom.multidimensional.index;

import org.junit.runner.RunWith;
import org.xenei.junit.contract.Contract.Inject;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.junit.contract.IProducer;

@ContractImpl(Tri4.class)
@RunWith(ContractSuite.class)
public class Tri4Test {
    @Inject
    public IProducer<Tri4> getProducer() {
        return new IProducer<Tri4>() {

            @Override
            public Tri4 newInstance() {
                return new Tri4(IndexTest.shape);
            }

            @Override
            public void cleanUp() {

            }
        };
    }

}
