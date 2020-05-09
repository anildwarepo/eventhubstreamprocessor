/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.anildwa

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import scala.util.matching.Regex

class EventHubListener() {

    @throws(classOf[IllegalArgumentException])
    def Start(filePath: String, fileFormat: String, eventHubConnectionString: String, eventHubTopic: String) : String  = {
         val spark = SparkSession.builder.getOrCreate

        
        // Define Taxi Data Schema
        val schema = (new StructType)
            .add("VendorID", StringType)
            .add("lpep_pickup_datetime", StringType)
            .add("lpep_dropoff_datetime", StringType)
            .add("store_and_fwd_flag", StringType)
            .add("ratecodeID", StringType)
            .add("PULocationID", StringType)
            .add("DOLocationID", StringType)
            .add("passenger_count", StringType)
            .add("trip_distance", StringType)
            .add("fare_amount", StringType)
            .add("extra", StringType)
            .add("mta_tax", StringType)
            .add("tip_amount", StringType)
            .add("tolls_amount", StringType)
            .add("ehail_fee", StringType)
            .add("improvement_surcharge", StringType)
            .add("total_amount", StringType)
            .add("payment_type", StringType)
            .add("trip_type", StringType)

    
        //Retrieve EventHub details from parameter and convert to regex
        val pattern = "Endpoint=sb://[A-Za-z0-9](?:[A-Za-z0-9\\-]{0,50}[A-Za-z0-9])?.servicebus.windows.net".r
        val matchEventHubString = pattern.findFirstIn(eventHubConnectionString)

        //Check if EventHub endpoint is correctly formatted and throw exception if it is correctly formatted
        if(matchEventHubString == None){
            throw new IllegalArgumentException("EventHubConnection String format is incorrect")
        }
        
        
        //Configure bootstrap servers for Azure Event Hub using Kafka Compatible Endpoint
        val BOOTSTRAP_SERVERS = matchEventHubString.getOrElse("").split("=")(1).split("sb://")(1) + ":9093"
        
        //Configure SASL connection string with EventHub connection info
        val EH_SASL = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\"  password=\"" + eventHubConnectionString +  ";EntityPath=" + eventHubTopic + "\";"

        
        //Create Kafka dataframe on the EventHub topic
        val kafkadf = spark.readStream.format("kafka").option("subscribe", eventHubTopic).option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS).option("kafka.sasl.mechanism", "PLAIN").option("kafka.security.protocol", "SASL_SSL").option("kafka.sasl.jaas.config", EH_SASL).option("kafka.request.timeout.ms", "60000").option("kafka.session.timeout.ms", "60000").option("failOnDataLoss", "false").load()



        // Un-comment the below three lines if using Azure HDInsight Kafka Cluster
        //val kafkaBrokers="10.1.0.7:9092,10.1.0.8:9092,10.1.0.9:9092"
        //val kafkaTopic="taxidatatopic"
        //val kafkaStreamDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", kafkaTopic).option("startingOffsets", "latest").option("groupId","ss-1").load()

        //Define the schema from kafka stream and write stream to file path with specified file format
        kafkadf.select(from_json(col("value").cast("string"), schema) as "trip").select("trip.VendorID", 
            "trip.lpep_pickup_datetime", 
            "trip.lpep_dropoff_datetime", 
            "trip.store_and_fwd_flag", 
            "trip.ratecodeID", 
            "trip.PULocationID", 
            "trip.DOLocationID", 
            "trip.passenger_count", 
            "trip.trip_distance", 
            "trip.fare_amount", 
            "trip.extra", 
            "trip.mta_tax", 
            "trip.tip_amount", 
            "trip.tolls_amount", 
            "trip.ehail_fee", 
            "trip.improvement_surcharge", 
            "trip.total_amount", 
            "trip.payment_type", 
            "trip.trip_type")
            .writeStream
            .format(fileFormat)
            .option("path", filePath + "/data")
            .option("checkpointLocation", filePath + "/streamcheckpoint")
            .option("failOnDataLoss","false")
            .start()
            .awaitTermination()          


        return "Stopped"
    }

    
}