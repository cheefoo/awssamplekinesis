package com.tayo.awssamplekinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

/**
 * Created by temitayo on 9/5/17.
 */
public class OrdinaryConsumer
{
    private static AmazonKinesis kinesis;

    final static CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    private static void init(String region)
    {
        kinesis = AmazonKinesisClientBuilder.standard().withRegion(region).build();
    }


    public static void main(String[] args)
    {
        if(args.length != 2)
        {
            System.out.println("USAGE: <stream_name> <aws_region>") ;
            System.exit(1);
        }

        String streamName = args[0];
        String region = args[1];

        init(region);
        DescribeStreamResult streamResult = kinesis.describeStream(streamName) ;

        StreamDescription streamDescription = streamResult.getStreamDescription();

        List<Shard> shardList = streamDescription.getShards();

        for(Shard shard: shardList)
        {
            String shardIterator = null;
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
            getShardIteratorRequest.setStreamName(streamName);
            getShardIteratorRequest.setShardId(shard.getShardId());
            getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
            GetShardIteratorResult getShardIteratorResult = kinesis.getShardIterator(getShardIteratorRequest);
            shardIterator = getShardIteratorResult.getShardIterator();
            List<Record> records;
            while(true)
            {
                GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
                getRecordsRequest.setShardIterator(shardIterator);
                getRecordsRequest.setLimit(25);

                GetRecordsResult result = kinesis.getRecords(getRecordsRequest);

               records = result.getRecords();
                if(records.size()>0)
                {
                    for(Record record: records)
                    {
                        try
                        {
                            System.out.println(decoder.decode(record.getData()).toString());
                        } catch (CharacterCodingException e)
                        {
                            System.out.println("Unable to decode data : "+record.getData().toString() + "with partition key " + record.getPartitionKey());
                            e.printStackTrace();
                        }
                    }
                }
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException exception)
                {
                    throw new RuntimeException(exception);
                }

                shardIterator = result.getNextShardIterator();
            }
        }

    }
}
