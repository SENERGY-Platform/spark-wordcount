package org.infai.senergy.benchmark.wordcount;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;


public class WordCount {
    public static void main(String[] args) {
        //Usage check
        String errorMessage = "Usage: org.infai.senergy.benchmark.wordcount.WordCount <logging> <hostlist> <inputTopic> <outputTopic> <sigma> <startingOffsets> <maxOffsetsPerTrigger> <shufflePartitions>\n" +
                "logging = boolean\n" +
                "hostlist = comma-separated list of kafka host:port\n" +
                "inputTopic = Topic will be subscribed to.\n" +
                "outputTopic = Output topic, where values will be written to\n" +
                "startingOffsets = Which Kafka Offset to use. Use earliest or latest\n" +
                "maxOffsetsPerTrigger = How many messages should be consumed at once (max)\n" +
                "shufflePartitions = How many shuffle partitions Spark should use (default is 200)";

        if (args.length != 7) {
            System.out.println(errorMessage);
            return;
        }
        //Parameter configuration
        boolean loggingEnabled;
        String hostlist, inputTopic, outputTopic, startingOffsets;
        int shufflePartitions;
        long maxOffsetsPerTrigger;
        try {
            loggingEnabled = Boolean.parseBoolean(args[0]);
            hostlist = args[1];
            inputTopic = args[2];
            outputTopic = args[3];
            startingOffsets = args[4];
            maxOffsetsPerTrigger = Long.parseLong(args[5]);
            shufflePartitions = Integer.parseInt(args[6]);
        } catch (Exception e) {
            System.out.println(errorMessage);
            return;
        }

        //Create Session
        SparkSession spark = SparkSession
                .builder()
                .appName("WordCount")
                .config("spark.eventLog.enabled", loggingEnabled)
                .config("spark.sql.shuffle.partitions", shufflePartitions)
                .getOrCreate();

        //Create DataSet representing the stream of input lines from kafka
        Dataset<Row> ds = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", hostlist)
                .option("subscribe", inputTopic)
                .option("startingOffsets", startingOffsets)
                .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
                .load();

        //Prepare the schema
        StructType schema =  new StructType().add("sentence", DataTypes.StringType);

        //Parse Kafka value to Dataframe (via json)
        Dataset<Row> df = ds.select(functions.from_json(ds.col("value").cast(DataTypes.StringType), schema)
                .as("data"))
                .select("data.*");

        Dataset<String> words = df.as(Encoders.STRING()).flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split("\\W+")).iterator(), Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        //Write wordcounts to kafka
        wordCounts.toJSON()
                .writeStream()
                .format("kafka")
                .option("checkpointLocation", "checkpoints/wordcount")
                .option("kafka.bootstrap.servers", hostlist)
                .option("topic", outputTopic)
                .start();

        // Wait for termination
        try {
            spark.streams().awaitAnyTermination();
        } catch (StreamingQueryException sqe) {
            System.out.println("Encountered StreamingQueryException while waiting for Termination. Did you try to exit the program?");
            sqe.printStackTrace();
        } finally {
            spark.close();
        }
    }
}
