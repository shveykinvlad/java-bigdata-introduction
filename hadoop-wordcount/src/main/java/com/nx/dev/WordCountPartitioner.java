package com.nx.dev;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner.
 */
public class WordCountPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        int length = value.getLength();

        if (numReduceTasks == 0) {
            return 0;
        }
        if (length <= 5) {
            return 0;
        } else {
            return 1 % numReduceTasks;
        }
    }
}
