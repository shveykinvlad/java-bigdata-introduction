package com.nx.dev;

import com.nx.dev.handler.DataFrameWordCountHandler;
import com.nx.dev.handler.RddWordCountHandler;
import com.nx.dev.handler.SparkSqlWordCountHandler;
import lombok.extern.log4j.Log4j2;
import org.apache.spark.SparkConf;

/**
 * Word counter.
 */
@Log4j2
public class Application {

    private static final String INPUT_PATH = "spark-wordcount/src/main/resources/words.txt";

    private static final String DATAFRAME_OUTPUT_PATH = "spark-wordcount/src/main/resources/dataframe";
    private static final String RDD_OUTPUT_PATH = "spark-wordcount/src/main/resources/rdd";
    private static final String SQL_OUTPUT_PATH = "spark-wordcount/src/main/resources/spark_sql";

    private static final String APP_NAME = "spark-word-count";
    private static final String LOCAL_MASTER = "local[*]";

    public static void main(String[] args) {
        final SparkConf sparkConfiguration = new SparkConf()
                .set("spark.driver.host", "127.0.0.1")
                .setAppName(APP_NAME)
                .setMaster(LOCAL_MASTER);

        RddWordCountHandler.execute(sparkConfiguration, INPUT_PATH, RDD_OUTPUT_PATH);
        DataFrameWordCountHandler.execute(sparkConfiguration, INPUT_PATH, DATAFRAME_OUTPUT_PATH);
        SparkSqlWordCountHandler.execute(sparkConfiguration, INPUT_PATH, SQL_OUTPUT_PATH);
    }
}
