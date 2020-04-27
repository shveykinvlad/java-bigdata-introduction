package com.nx.dev.handler;

import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Spam words handler.
 */
@UtilityClass
@Log4j2
public class SpamWordsHandler {

    private static final String DELIMITERS = "[ \t\n.,\\-()\\[\\]='+:?!\"%&*<>;{}@#_№|^$«»/]";
    private static final String HAM_TAG = "ham";
    private static final String SPAM_TAG = "spam";
    private static final ComparatorByValue COMPARATOR_BY_VALUE = new ComparatorByValue();

    /**
     * Finds top-5 spam words are not contained in ham messages.
     *
     * @param sparkContext spark context
     * @param path         path to file
     * @return List of tuples (word, count)
     */
    public static List<Tuple2<String, Integer>> execute(final JavaSparkContext sparkContext, String path) {
        JavaRDD<String> lines = sparkContext.textFile(path, 1)
                .cache();

        JavaPairRDD<String, String> linesWithTag = lines
                .mapToPair(mapByTag())
                .flatMapValues(lineToWords());

        JavaRDD<String> hamWords = linesWithTag
                .filter(pair -> HAM_TAG.equals(pair._1))
                .map(pair -> pair._2);

        JavaRDD<String> spamWords = linesWithTag
                .filter(pair -> SPAM_TAG.equals(pair._1))
                .map(pair -> pair._2)
                .subtract(hamWords);

        JavaPairRDD<String, Integer> counts = spamWords
                .mapToPair(word -> new Tuple2<>(word.toLowerCase(), 1))
                .reduceByKey(Integer::sum);

        return counts.top(5, COMPARATOR_BY_VALUE);
    }

    private static PairFunction<String, String, String> mapByTag() {
        return line -> {
            if (line.startsWith(HAM_TAG)) {
                return new Tuple2<>(HAM_TAG, line);
            } else {
                return new Tuple2<>(SPAM_TAG, line);
            }
        };
    }

    private static Function<String, Iterable<String>> lineToWords() {
        return line -> {
            String[] lineWords = line.toLowerCase().split(DELIMITERS);
            String[] messageWords = Arrays.copyOfRange(lineWords, 1, lineWords.length);

            return Arrays.asList(messageWords);
        };
    }

    /**
     * Comparator by value.
     */
    private static class ComparatorByValue implements Comparator<Tuple2<String, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<String, Integer> tuple1,
                           Tuple2<String, Integer> tuple2) {
            return tuple1._2 - tuple2._2;
        }
    }
}
