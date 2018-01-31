package com.aws.xaccount;

import com.amazonaws.auth.*;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class CrossAccountAccess
{
    private static AmazonKinesis kinesis;
    public static void main(String [] args)
    {
        if(args.length != 3)
        {
            System.out.println("Usage : CrossAccountAccess <stream_name> <region> <role_arn> " );
        }

        String streamName = args[0];
        String region = args[1];
        String roleArn = args[2];

        AWSCredentials longTermCredentials = new DefaultAWSCredentialsProviderChain().getCredentials();

        AWSSecurityTokenServiceClient securityTokenServiceClient = (AWSSecurityTokenServiceClient) AWSSecurityTokenServiceClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(longTermCredentials)).build();

        AssumeRoleRequest assumeRequest = new AssumeRoleRequest().withRoleArn(roleArn).withDurationSeconds(3600).withRoleSessionName("Test");

        AssumeRoleResult assumeResult = securityTokenServiceClient.assumeRole(assumeRequest);

        System.out.println("Access Key is " + assumeResult.getCredentials().getAccessKeyId());


        AWSCredentials temporaryCredentials;

        temporaryCredentials = new BasicSessionCredentials
                (assumeResult.getCredentials().getAccessKeyId(), assumeResult.getCredentials().getSecretAccessKey(), assumeResult.getCredentials().getSessionToken());


        kinesis = AmazonKinesisClientBuilder.standard().withRegion(region).withCredentials(new AWSStaticCredentialsProvider(temporaryCredentials)).build();
        System.out.println("Client obtained successfully");


        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);

        DescribeStreamResult result = kinesis.describeStream(describeStreamRequest);

        System.out.println(result.toString());

        for (int i = 0; i < 100; i++)
        {
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(streamName);
            try
            {
                putRecordRequest.setData(ByteBuffer.wrap(String.format("TestData").getBytes("UTF-8")));
            } catch (UnsupportedEncodingException e)
            {
                e.printStackTrace();
            }
            putRecordRequest.setPartitionKey("Test");
            PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
            System.out.println(i + "with seq number" + putRecordResult.getSequenceNumber());
        }


        java.util.List<Shard> shardList = result.getStreamDescription().getShards();

        java.util.List<Record> records;
        for(Shard shard: shardList)
        {
            String shardIterator = null;
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
            getShardIteratorRequest.setStreamName(streamName);
            getShardIteratorRequest.setShardId(shard.getShardId());
            getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
            GetShardIteratorResult gsr = kinesis.getShardIterator(getShardIteratorRequest);
            shardIterator = gsr.getShardIterator();

            GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
            getRecordsRequest.setShardIterator(shardIterator);
            getRecordsRequest.setLimit(50);

            GetRecordsResult getRecordsResult = kinesis.getRecords(getRecordsRequest);

            records = getRecordsResult.getRecords();
            if(records.size()>1)
            {
                System.out.println("Received " + records.size() + " records from shard " + shard);

            }
            else
            {
                System.out.println("Received " + records.size() + " records from shard " + shard);
            }


        }


    }
}
