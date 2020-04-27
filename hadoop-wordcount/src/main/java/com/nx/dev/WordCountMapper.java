package com.nx.dev;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Mapper.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final List<String> FILTER_WORDS = Arrays.asList("a", "of", "and", "", "the", "to");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();
        StringTokenizer words = new StringTokenizer(text, " .,?!;:(){}[]");
        while (words.hasMoreTokens()) {
            String word = words.nextToken().toLowerCase();
            if (!FILTER_WORDS.contains(word)) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }
}
