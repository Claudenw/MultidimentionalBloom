package org.xenei.bloom.multidimensional.index;

import org.junit.runner.RunWith;
import org.xenei.junit.contract.Contract.Inject;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.junit.contract.IProducer;

@ContractImpl(FlatBloofi.class)
@RunWith(ContractSuite.class)
public class FlatBloofiTest {

    @Inject
    public IProducer<FlatBloofi> getProducer() {
        return new IProducer<FlatBloofi>() {

            @Override
            public FlatBloofi newInstance() {
                return new FlatBloofi(IndexTest.SHAPE);
            }

            @Override
            public void cleanUp() {

            }
        };
    }

}
