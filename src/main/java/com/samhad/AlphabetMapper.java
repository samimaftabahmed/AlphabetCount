package com.samhad;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class AlphabetMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static IntWritable INT_WRITABLE = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        int tokenLength = 0;

        while (stringTokenizer.hasMoreTokens()) {

            tokenLength = stringTokenizer.nextToken().trim().length();

            if (tokenLength > 0) {
                word.set(String.valueOf(tokenLength));
                context.write(word, INT_WRITABLE);
            }
        }
    }
}
