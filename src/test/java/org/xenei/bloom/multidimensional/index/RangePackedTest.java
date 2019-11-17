package org.xenei.bloom.multidimensional.index;

import org.junit.runner.RunWith;
import org.xenei.junit.contract.Contract.Inject;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.junit.contract.IProducer;

@ContractImpl(RangePacked.class)
@RunWith(ContractSuite.class)
public class RangePackedTest {

    @Inject
    public IProducer<RangePacked> getProducer() {
        return new IProducer<RangePacked>() {

            @Override
            public RangePacked newInstance() {
                return new RangePacked(IndexTest.shape);
            }

            @Override
            public void cleanUp() {

            }
        };
    }

}
