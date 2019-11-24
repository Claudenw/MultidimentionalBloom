package org.xenei.bloom.multidimensional.index;

import org.junit.runner.RunWith;
import org.xenei.junit.contract.Contract.Inject;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.junit.contract.IProducer;

@ContractImpl(Linear.class)
@RunWith(ContractSuite.class)
public class LinearTest {

    @Inject
    public IProducer<Linear> getProducer() {
        return new IProducer<Linear>() {

            @Override
            public Linear newInstance() {
                return new Linear(IndexTest.SHAPE);
            }

            @Override
            public void cleanUp() {

            }
        };
    }

}
