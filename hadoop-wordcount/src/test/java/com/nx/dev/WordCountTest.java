package com.nx.dev;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for {@link WordCountMapper}, {@link WordCountReducer}.
 */
public class WordCountTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordCountMapper mapper = new WordCountMapper();
        WordCountReducer reducer = new WordCountReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer, reducer);
    }

    @Test
    public void map() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("2 b"));
        mapDriver.withInput(new LongWritable(), new Text("or not 2 b"));

        mapDriver.withOutput(new Text("2"), new IntWritable(1));
        mapDriver.withOutput(new Text("b"), new IntWritable(1));
        mapDriver.withOutput(new Text("or"), new IntWritable(1));
        mapDriver.withOutput(new Text("not"), new IntWritable(1));
        mapDriver.withOutput(new Text("2"), new IntWritable(1));
        mapDriver.withOutput(new Text("b"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void reduce() throws IOException {
        List<IntWritable> values1 = new ArrayList<>();
        values1.add(new IntWritable(1));
        reduceDriver.withInput(new Text("not"), values1);

        List<IntWritable> values3 = new ArrayList<>();
        values3.add(new IntWritable(1));
        reduceDriver.withInput(new Text("or"), values3);

        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("2"), values);

        List<IntWritable> values2 = new ArrayList<>();
        values2.add(new IntWritable(1));
        values2.add(new IntWritable(1));
        reduceDriver.withInput(new Text("b"), values2);

        reduceDriver.withOutput(new Text("not"), new IntWritable(1));
        reduceDriver.withOutput(new Text("or"), new IntWritable(1));
        reduceDriver.withOutput(new Text("2"), new IntWritable(2));
        reduceDriver.withOutput(new Text("b"), new IntWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void mapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "2 b"));
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "or not 2 b"));

        mapReduceDriver.withOutput(new Text("2"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("b"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("not"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("or"), new IntWritable(1));

        mapReduceDriver.runTest();
    }
}