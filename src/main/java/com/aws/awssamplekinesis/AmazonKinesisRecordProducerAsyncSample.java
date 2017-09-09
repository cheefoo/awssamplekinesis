package com.aws.awssamplekinesis;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.amazonaws.services.kinesis.model.*;




public class AmazonKinesisRecordProducerAsyncSample
{

    static AmazonKinesisAsync kinesis;
   // private static  List<String> dataList;
    //Todo: download File from S3 and write to FileSystem
    private static final String filePath =  "/Users/temitayo/workspace/awssamplekinesis/scripts/watch/4ff1ddf1-5d30-41a8-bc89-f08d8a8b6d0d.json";
    static List<String> dataList = new ArrayList<>();
    private static void init(String region) throws Exception
    {
         kinesis = AmazonKinesisAsyncClientBuilder.standard().withRegion(region).build();

    }

    public AmazonKinesisRecordProducerAsyncSample(String region)
    {
        kinesis = AmazonKinesisAsyncClientBuilder.standard().withRegion(region).build();
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
        dataList = DataUtils.retrieveDataLines(filePath);
        putMultipleRecordBatch(myStreamName, dataList);
    }

    private static void putMultipleRecordBatch(String myStreamName, List<String> dataList) throws Exception
    {
        PutRecordsRequest putRecordsRequest;
        List<PutRecordsRequestEntry> putRecordsRequestEntryList;
        int batchRequestCount = 1;

        List<String> dataList2 =  dataList;
        while (true)
        {
            putRecordsRequestEntryList = new ArrayList<>();
            for(int i = 0; i < dataList.size(); i++ )
            {
                long createTime = System.currentTimeMillis();
                PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
                putRecordsRequestEntry.setData(ByteBuffer.wrap(dataList2.get(i).getBytes("UTF-8")));
                putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", createTime));
                putRecordsRequestEntryList.add(putRecordsRequestEntry);

                if(i%499==0)
                {
                    putRecordsRequest = new PutRecordsRequest();
                    putRecordsRequest.setStreamName(myStreamName);
                    putRecordsRequest.setRecords(putRecordsRequestEntryList);
                    Future<PutRecordsResult> putRecordsResult = kinesis.putRecordsAsync(putRecordsRequest, new AKRPASCallBackHandler()) ;
                    System.out.println("Successfully sent put record batch number : " + batchRequestCount + " with result " + putRecordsResult.toString());
                    putRecordsRequestEntryList = new ArrayList<>();
                    batchRequestCount++;
                }


            }
            dataList2 = dataList;

        }
    }


}