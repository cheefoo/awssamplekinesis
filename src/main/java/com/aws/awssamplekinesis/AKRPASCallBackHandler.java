package com.aws.awssamplekinesis;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aws.awssamplekinesis.AmazonKinesisRecordProducerAsyncSample.kinesis;

/**
 * Created by temitayo on 9/9/17.
 */
//Assumption: Objects are idempotent and can be read multiple times by consumers without affecting results
//Todo: Filter and send only failed records
public class AKRPASCallBackHandler  implements AsyncHandler<PutRecordsRequest, PutRecordsResult>
{
    static AtomicInteger errorCount = new AtomicInteger(0);

    @Override
    public void onError(Exception e)
    {
        System.out.println("Non 200K" + errorCount.getAndIncrement());
    }

    @Override
    public void onSuccess(PutRecordsRequest request, PutRecordsResult putRecordsResult)
    {
        System.out.println("Waiting for future");
        AtomicInteger attemptCount = new AtomicInteger(0);
         try
            {

                if(putRecordsResult.getFailedRecordCount() > 0  && containsFailureErrorCode(putRecordsResult.getRecords(), "InternalFailure"))    // Not retrying wpte
                {
                    //backoff a bit and then retry batch
                    Thread.sleep(100);
                    System.out.println("retrying failed batch") ;
                    Future<PutRecordsResult> putRecordsResult2 = kinesis.putRecordsAsync(request,  new AKRPASCallBackHandler()) ;
                    System.out.println("Retried successfully" + attemptCount.getAndIncrement() + putRecordsResult2.toString());

                }
                else
                {
                    System.out.println("All Clear");
                }
            }
            catch(InterruptedException ie)
            {
                System.out.println(ie.toString());
            }

    }

    public static boolean containsFailureErrorCode(List<PutRecordsResultEntry> list, String error) {
        for (PutRecordsResultEntry object : list) {
            if (object.getErrorCode() == error)
            {
                return true;
            }
        }
        return false;
    }
}
