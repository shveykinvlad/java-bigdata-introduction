package com.nx.dev.handler;

import lombok.experimental.UtilityClass;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;


/**
 * Word count handler with DataFrame approach.
 */
@UtilityClass
public class DataFrameWordCountHandler {

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
        try (SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()) {
            Dataset<Row> lines = sparkSession.read()
                    .textFile(inputPath)
                    .toDF("line")
                    .cache();
            Dataset<Row> words = lines
                    .flatMap(lineToWords(), Encoders.STRING())
                    .toDF("word");
            Dataset<Row> filterWords = sparkSession.createDataset(FILTER_WORDS, Encoders.STRING()).toDF("filter_word");

            Dataset<Row> filteredWords = words.join(
                    filterWords,
                    filterWords.col("filter_word").equalTo(words.col("word")),
                    "left_anti");
            Dataset<Row> wordsWithCounts = filteredWords
                    .groupBy("word")
                    .count();
            wordsWithCounts.write().parquet(outputPath);
        }
    }

    private static FlatMapFunction<Row, String> lineToWords() {
        return line ->
                Arrays.asList(line
                        .toString()
                        .toLowerCase()
                        .split(DELIMITERS))
                        .iterator();
    }
}

