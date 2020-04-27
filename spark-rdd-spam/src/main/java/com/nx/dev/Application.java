package com.nx.dev;

import com.nx.dev.handler.SpamWordsHandler;
import lombok.extern.log4j.Log4j2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Finds top-5 spam words are not contained in ham messages.
 * Uses RDD approach
 */
@Log4j2
public class Application {

    private static final String PATH = "spark-rdd-spam/src/main/resources/smsData.txt";
    private static final String APP_NAME = "spark-rdd-spam";
    private static final String LOCAL_MASTER = "local[*]";

    public static void main(String[] args) {
        final SparkConf sparkConfiguration = new SparkConf()
                .set("spark.driver.host", "127.0.0.1")
                .setAppName(APP_NAME)
                .setMaster(LOCAL_MASTER);
        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConfiguration);
        sparkContext.setLogLevel("ERROR");

        log.info(SpamWordsHandler.execute(sparkContext, PATH));
    }
}
