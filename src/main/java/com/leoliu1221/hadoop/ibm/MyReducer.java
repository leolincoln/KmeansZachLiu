package com.leoliu1221.hadoop.ibm;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text text, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0.0;
		double count = 0.0;
		for (DoubleWritable value : values) {
			sum += value.get();
			count += 1.0;
		}
		context.write(text, new DoubleWritable(sum / count));
	}
}