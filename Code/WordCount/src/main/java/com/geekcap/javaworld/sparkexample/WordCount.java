/**
* Illustrates a wordcount in Java
*/
package com.geekcap.javaworld.sparkexample;
import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;
import scala.Tuple2;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
public class WordCount {
	public static void main(String[] args) throws Exception {
			
		String inputFile = args[0];
		String outputFile = args[1];
		
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);

		// Split up into words.
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String line) {
				return Arrays.asList(line.split(" "));
			}
		});

		// Map each word to a (word, 1) pair
		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>(){
			public Tuple2<String, Integer> call(String x){
				return new Tuple2(x, 1);
			}
		});

		// ReduceByKey to count the occurrences of each word
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>(){
			public Integer call(Integer x, Integer y){
				return x + y;
			}
		});

		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputFile);
	}
}
