#!/usr/bin/python
# This script will generate Json data on demand
#{
 # "CLIENT_ID"
# "TICKER": "AMZN",
 #   "TxnType: "BUY"
#    "salePrice": "89.00",
#    "orderId": "F",
#    "activityTimestamp": "2016-12-07 11:46:29"
#}

from faker import Factory
import uuid, sys, time, csv, json, os, random
from time import gmtime, strftime

errUsage = "Usage: " + sys.argv[0] + " [number-runs] [rumber-rows]"
errEg = " -> eg: " + sys.argv[0] + " 10 100000"

# Basic Args Check and parse
if sys.argv[0] == "" and sys.argv[1] == "help":
    print(errUsage)
    print(errEg)
    exit(-1)

if len(sys.argv) != 3:
    print(errUsage)
    print(errEg)
    exit(-1)

numberRuns = int(sys.argv[1])
numberRows = int(sys.argv[2])

ticker=["CHY","USD","GBY","EUR","NGN","CAD","CNY","JPY"]
transaction = ["BUY", "SELL"]

#relationshipStatus = ["single", "in a relationship", "married", "engaged", "divorced", "have cats"]
#activityType = ["CommentAdded", "CommentRemoved", "TopicViewed", "ProfileUpdated", "CommentLiked", "CommentDisliked", "ProfileCreated"]
#sex = ["M", "F", "O"]
targetDir = './generatedData'
putDir = './watch'
archiveDir = './archiveDir'

#Directory which the KPL watches
if not os.path.exists(putDir):
    os.mkdir(putDir)

#Directory which the KPL archives read file
if not os.path.exists(archiveDir):
    os.mkdir(archiveDir)

if __name__ == "__main__":
    # Generate data into multiple files into a sub directory called "generatedData"
    if not os.path.exists(targetDir):
        os.mkdir(targetDir)
    for y in xrange(numberRuns):
        timestart = time.strftime("%Y%m%d%H%M%S")
        destFile = str(uuid.uuid4()) + ".json"
        file_object = open(targetDir + "/" + destFile,"a")

        def create_names(fake):
            for x in range(numberRows):
                genUserId = fake.email()
                symbol = ticker[random.randint(0,7)]
                txnType =  transaction[random.randint(0,1)]
                salesPrice =  random.uniform(2,1000)
                orderId = str(uuid.uuid4())
                activityTimestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())



                #file_object.write('{"symbol": "' + symbol + '", "salesPrice": "' + str(salesPrice) + '","orderId": "' + orderId + '","activityTimestamp": "' + activityTimestamp  + '"}\n')
                file_object.write( genUserId + "," + symbol + ", "  + txnType + "," + str(salesPrice) + "," + orderId + ", " + activityTimestamp  + '\n')



        if __name__ == "__main__":
            fake = Factory.create()
            create_names(fake)
            #create_names()
            file_object.close()
            naptime=random.randint(1,5)
            print "generated " + str(numberRows) + " records into " + targetDir + "/" + destFile
            print "sleeping for " + str(naptime) + " seconds"
            os.rename(targetDir+"/"+destFile, putDir+"/"+destFile);
            time.sleep(naptime)

    print("\ngenerated: " + str(numberRuns) + " files, " + "with " + str(numberRows) + " records each\n" )
