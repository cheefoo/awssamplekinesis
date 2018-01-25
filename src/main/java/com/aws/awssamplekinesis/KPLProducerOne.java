package com.aws.awssamplekinesis;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.producer.*;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.Collectors;


/**
 * Created by temitayo on 8/4/17.
 */
public class KPLProducerOne
{
    private static final Logger log = LoggerFactory.getLogger(KPLProducerOne.class);

    private static KinesisProducer getKinesisProducer(String region) throws IOException
    {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration();
        config.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());
        config.setMaxConnections(24);
        config.setRequestTimeout(60000);
        config.setRecordMaxBufferedTime(15000);
        config.setRegion(region);
        config.setMetricsNamespace("KPL_Lessons");
        config.setAggregationEnabled(true);
        return new KinesisProducer(config);
    }


    public static void main (String [] args) throws IOException
    {
        if(args.length != 2)
        {
            System.out.println("USAGE: <stream_name> <aws_region>") ;
            System.exit(1);
        }

        String streamName = args[0];
        String region = args[1];
        KinesisProducer producer = getKinesisProducer(region);

        final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>()
        {
            @Override
            public void onFailure(Throwable t)
            {

                if (t instanceof UserRecordFailedException)
                {
                    UserRecordFailedException e =
                            (UserRecordFailedException) t;
                    UserRecordResult result = e.getResult();

                    String errorList =
                            StringUtils.join(result.getAttempts().stream()
                                    .map(a -> String.format(
                                            "Delay after prev attempt: %d ms, "
                                                    + "Duration: %d ms, Code: %s, "
                                                    + "Message: %s",
                                            a.getDelay(), a.getDuration(),
                                            a.getErrorCode(),
                                            a.getErrorMessage()))
                                    .collect(Collectors.toList()), "n");

                    log.error(String.format("Record failed to put, attempts:n%s ",  errorList));

                }
                log.error("Exception during put", t);

            }

            @Override
            public void onSuccess(UserRecordResult userRecordResult)
            {

                long totalTime = userRecordResult.getAttempts().stream()
                        .mapToLong(a -> a.getDelay() + a.getDuration())
                        .sum();
                log.info(String.format(
                        "Succesfully put record,  "
                                + " sequenceNumber=%s, "
                                + "shardId=%s, took %d attempts, "
                                + "totalling %s ms",
                        userRecordResult.getSequenceNumber(),
                        userRecordResult.getShardId(), userRecordResult.getAttempts().size(),
                        totalTime));

            }
        };


        while(true)
        {
            try
            {
                long createTime = System.currentTimeMillis();
                ByteBuffer data = ByteBuffer.wrap(String.format("testData-%d", createTime).getBytes("UTF-8"));
                ListenableFuture<UserRecordResult> f = producer.addUserRecord(streamName, DataUtils.randomPartitionKey(), data);
                Thread.sleep(1);
                Futures.addCallback(f, callback);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }



         }

    }


}
