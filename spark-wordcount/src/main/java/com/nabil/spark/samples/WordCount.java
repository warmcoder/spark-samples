package com.nabil.spark.samples;

import java.util.Arrays;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @author nabil
 */
public class WordCount {

	public static void main(String[] args) {

		// Getting the input file path. example : hdfs://path_to_file.txt ...
		// For testing on your local machine, try inputs/lorem-ipsum.txt ...
		
		final String inputTextFilePath = getInputTextFilePath(); 

		// Getting the output file path
		
		final String outputTextFilePath = getOutputTextFilePath();

		// Creating Spark Context ...

		SparkConf conf = new SparkConf().setAppName("Nabil Word Count").setMaster("local").set("spark.cores.max", "10");

		JavaSparkContext sc = new JavaSparkContext(conf);

		// Creating text file RDDs ...

		JavaRDD<String> lines = sc.textFile(inputTextFilePath);

		// Transforming RDDs
		
		JavaRDD<String> words = lines.flatMap(line ->Arrays.asList(line.split(" ")).iterator());
		
		// Mapping ...
		
		JavaPairRDD<String, Integer> counts = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));

		// Reducing ...
		
		JavaPairRDD<String, Integer> wordCounts = counts.reduceByKey((x, y) -> x + y);

		// Saving the result ...
		wordCounts.saveAsTextFile(outputTextFilePath);

		// Closing the spark context ...
		
		sc.close();
	}

	/**
	 * @return the input text file path 
	 */
	@SuppressWarnings("resource")
	private static String getInputTextFilePath() {
		System.out.println("Input text file path : ");
		return new Scanner(System.in).nextLine();
	}

	/**
	 * @return the output text file path 
	 */
	@SuppressWarnings("resource")
	private static String getOutputTextFilePath() {
		System.out.println("Output text file path : ");
		return new Scanner(System.in).nextLine();
	}

}
