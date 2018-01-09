package com.aws.awssamplekinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class SimulatingIteratorAgeProceessFactory  implements IRecordProcessorFactory
{

    public IRecordProcessor createProcessor() {
        return new SimulationKinesisAppRP();
    }
}
