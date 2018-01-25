package com.aws.samplefirehose;




import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LobBigFilesAtKinesis
{
    private static AmazonKinesisFirehose firehose;
    private static final String REGION = "us-west-2";
    private static final String STREAM_NAME = "TayoSparkStream";
    private static final String DELIVERY_STREAM_NAME = "AuroraChangesToS3";
    private static void init(String region) throws Exception
    {
        firehose = AmazonKinesisFirehoseClientBuilder.standard().withRegion(region).build();

    }

        private static final String filePath = "/Users/temitayo/workspace/awssamplekinesis/scripts/watch/8d15d1e5-7e79-4e72-9958-5fa7baed8a9e.json";

        //private static final String filePath = "/Users/temitayo/workspace/awssamplekinesis/scripts/watch/ef5885e9-544c-4da5-9b93-e796b0d41af7.json";
        static String readFile(String path, Charset encoding) throws IOException
        {
            byte[] encoded = Files.readAllBytes(Paths.get(path));
            return new String(encoded, encoding);
        }

        public static void main (String [] args) throws Exception
        {
            init(REGION);

	    	Charset encoding = StandardCharsets.UTF_8.defaultCharset();

	    	//Instrumentation instrument;
            int numOfPuts=10000;
            String record = null;
            try
            {
                record = readFile(filePath, encoding);
                //firehose specific
                Record firehoseRecord = new Record().withData(ByteBuffer.wrap(String.format(record).getBytes("UTF-8")));

                System.out.println("Record size before put is " + calculateSizeOfObject(record));
                //System.out.println("Size of Record is :" + record.)
                PutRecordRequest putRecordRequest = new PutRecordRequest();
                putRecordRequest.setDeliveryStreamName(DELIVERY_STREAM_NAME);
                //put random data
                putRecordRequest.setRecord(firehoseRecord);
                //putRecordRequest.setPartitionKey(DataUtils.randomPartitionKey());
                PutRecordResult putRecordResult = firehose.putRecord(putRecordRequest);
                System.out.println("Record Id is " + putRecordResult.getRecordId());
            }
            catch(UnsupportedEncodingException uee)
            {
                System.out.println("Exception Chief is mine Interrupted Exception");
                uee.printStackTrace();

            }
            catch (IOException e) {
                // TODO Auto-generated catch block
                System.out.println("Exception Chief is mine in reading file");
                e.printStackTrace();
                System.exit(1);
            }

            System.out.println("Done");
        }

        public static int calculateSizeOfObject(Object obj) throws IOException
        {
            ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);

            objectOutputStream.writeObject(obj);
            objectOutputStream.flush();
            objectOutputStream.close();

            return byteOutputStream.toByteArray().length;

        }

    }


