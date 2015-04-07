package com.leoliu1221.hadoop.ibm;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

//	private final IntWritable ONE = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		//check if the last column is false. 
		String[] data = value.toString().split(",");
		if(!data[data.length-1].toLowerCase().equals("false")) return;
		
		
		DoubleWritable four = new DoubleWritable();
		
		//combine the 30 31 32 33 into a unque id where if they dont have the same combination they wont be the same. 
		String id = data[30]+','+data[31]+','+data[32]+','+data[33]+',';

		
		word.set(id);
		four.set(Double.parseDouble(data[4]));
		context.write(word, four);

	}
}