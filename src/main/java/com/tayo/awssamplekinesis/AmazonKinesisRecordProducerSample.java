package com.tayo.awssamplekinesis;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;


import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamDescription;

import javax.xml.crypto.Data;


public class AmazonKinesisRecordProducerSample
{

    private static AmazonKinesis kinesis;
    private static final String filePath =  "/Users/temitayo/workspace/awssamplekinesis/scripts/watch/4ff1ddf1-5d30-41a8-bc89-f08d8a8b6d0d.json";

    private static void init(String region) throws Exception
    {
        kinesis = AmazonKinesisClientBuilder.standard().withRegion(region).build();
    }

    public static void main(String[] args) throws Exception
    {
        if(args.length != 2)
        {
            System.out.println("USAGE: <stream_name> <aws_region>") ;
            System.exit(1);
        }


        String region = args[1];
        init(region);

        final String myStreamName = args[0];

        final Integer myStreamSize = 1;

        // Describe the stream and check if it exists.
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(myStreamName);

        try {

            StreamDescription streamDescription =  kinesis.describeStream(describeStreamRequest).getStreamDescription();
           // DescribeStreamResult result = kinesis.describeStream(describeStreamRequest);
            System.out.printf("Stream %s has a status of %s.\n", myStreamName, streamDescription.getStreamStatus());

            if ("DELETING".equals(streamDescription.getStreamStatus())) {
                System.out.println("Stream is being deleted. This sample will now exit.");
                System.exit(0);
            }

            // Wait for the stream to become active if it is not yet ACTIVE.
            if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
                waitForStreamToBecomeAvailable(myStreamName);
            }
        } catch (ResourceNotFoundException ex) {
            System.out.printf("Stream %s does not exist. Creating it now.\n", myStreamName);

            // Create a stream. The number of shards determines the provisioned throughput.
            CreateStreamRequest createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(myStreamName);
            createStreamRequest.setShardCount(myStreamSize);
            kinesis.createStream(createStreamRequest);
            // The stream is now being created. Wait for it to become active.
            waitForStreamToBecomeAvailable(myStreamName);
        }

        // List all of my streams.
        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        listStreamsRequest.setLimit(10);
        ListStreamsResult listStreamsResult = kinesis.listStreams(listStreamsRequest);
        List<String> streamNames = listStreamsResult.getStreamNames();
        while (listStreamsResult.isHasMoreStreams()) {
            if (streamNames.size() > 0) {
                listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size() - 1));
            }

            listStreamsResult = kinesis.listStreams(listStreamsRequest);
            streamNames.addAll(listStreamsResult.getStreamNames());
        }
        // Print all of my streams.
        System.out.println("List of my streams: ");
        for (int i = 0; i < streamNames.size(); i++) {
            System.out.println("\t- " + streamNames.get(i));
        }

        System.out.printf("Putting records in stream : %s until this application is stopped...\n", myStreamName);
        System.out.println("Press CTRL-C to stop.");
        // Write records to the stream until this program is aborted.
        List<String> dataList = DataUtils.retrieveDataLines(filePath);
        while (true)
        {
            for(int i = 0; i < dataList.size(); i++ )
            {
                long createTime = System.currentTimeMillis();
                PutRecordRequest putRecordRequest = new PutRecordRequest();
                putRecordRequest.setStreamName(myStreamName);
                putRecordRequest.setData(ByteBuffer.wrap(dataList.get(i).getBytes("UTF-8")));
                putRecordRequest.setPartitionKey(String.format("partitionKey-%d", createTime));
                System.out.println("Putting record data : " + dataList.get(i));
                PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
                System.out.printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
                        putRecordRequest.getPartitionKey(),
                        putRecordResult.getShardId(),
                        putRecordResult.getSequenceNumber());
            }
          dataList = DataUtils.retrieveDataLines(filePath);
        }
    }

    private static void waitForStreamToBecomeAvailable(String myStreamName) throws InterruptedException {
        System.out.printf("Waiting for %s to become ACTIVE...\n", myStreamName);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));

            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(myStreamName);
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = kinesis.describeStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                System.out.printf("\t- current state: %s\n", streamStatus);
                if ("ACTIVE".equals(streamStatus)) {
                    return;
                }
            } catch (ResourceNotFoundException ex) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.
            } catch (AmazonServiceException ase) {
                throw ase;
            }
        }

        throw new RuntimeException(String.format("Stream %s never became active", myStreamName));
    }
}