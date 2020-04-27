package com.nx.dev.handler;

import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Word count handler with Spark SQL approach.
 */
@Log4j2
@UtilityClass
public class SparkSqlWordCountHandler {

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
            Dataset<String> lines = sparkSession.read()
                    .textFile(inputPath)
                    .cache();
            Dataset<String> words = lines
                    .flatMap(lineToWords(), Encoders.STRING());
            words.createTempView("word");

            Dataset<String> filterWords = sparkSession.createDataset(FILTER_WORDS, Encoders.STRING());
            filterWords.createTempView("filter_word");

            Dataset<Row> wordsWithCounts = sparkSession.sql(
                    "SELECT value, count(1) FROM word WHERE value NOT IN (SELECT * from filter_words) GROUP BY VALUE");
            wordsWithCounts.write().parquet(outputPath);
        } catch (AnalysisException e) {
            log.error(e);
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
