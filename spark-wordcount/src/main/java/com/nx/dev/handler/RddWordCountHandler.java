package com.nx.dev.handler;

import lombok.experimental.UtilityClass;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Word count handler with RDD approach.
 */
@UtilityClass
public class RddWordCountHandler {

    private static final String DELIMITERS = "[ \t\n.,\\-()\\[\\]='+:?!\"%&*<>;{}@#_№|^$«»/]";
    private static final List<String> FILTER_WORDS = Arrays.asList("a", "of", "and", "", "the", "to");

    /**
     * Makes words statistic.
     *
     * @param sparkConf  spark configuration
     * @param inputPath  path to input source text
     * @param outputPath path to output statistic
     */
    public static void execute(final SparkConf sparkConf, String inputPath, String outputPath) {
        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> lines = sparkContext.textFile(inputPath, 1)
                    .cache();

            JavaRDD<String> words = lines
                    .flatMap(lineToWords())
                    .subtract(sparkContext.parallelize(FILTER_WORDS));

            JavaPairRDD<String, Integer> counts = words
                    .mapToPair(word -> new Tuple2<>(word.toLowerCase(), 1))
                    .reduceByKey(Integer::sum);

            counts.saveAsTextFile(outputPath);
        }
    }

    private static FlatMapFunction<String, String> lineToWords() {
        return line ->
                Arrays.asList(line
                        .toLowerCase()
                        .split(DELIMITERS))
                        .iterator();
    }
}
