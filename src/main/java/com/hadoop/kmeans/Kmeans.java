package com.hadoop.kmeans;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Kmeans {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		String fileName1 = "/huser54/hadoopJobs/hw3/old_centroids.txt";
		// String fileName2 = "/huser54/hadoopJobs/hw3/new_centroids.txt";

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = Job.getInstance(conf);
		// Job job = new Job(conf, "MaxTemperature");
		job.setJarByClass(Kmeans.class);
		job.setJobName("KmeansZachLiu");
		// Setup MapReduce
		// job.setCombinerClass(MyCombiner.class);
		job.setMapperClass(MyMapper.class);
		job.setNumReduceTasks(1);
		// job.setReducerClass(MyReducer.class);
		// job.setNumReduceTasks(1);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.addCacheFile(new Path(fileName1).toUri());
		// job.addCacheFile(new Path(fileName2).toUri());

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);

	}

	public static class Centroid implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 3998692641498513263L;
		double[] corr;
		String corrString;

		public Centroid(String input) {
			String[] inputcorr = input.split(",");
			corr = new double[inputcorr.length];
			for (int i = 0; i < inputcorr.length; i++) {
				corr[i] = Double.parseDouble(inputcorr[i]);
			}
			this.corrString = input;
		}

		public Centroid(Double[] input) {
			this(StringUtils.join(input, ","));
		}

		public Centroid(String[] input) {
			this(StringUtils.join(input, ","));
		}

		public String toString() {
			return corrString;
		}

		public boolean equals(Centroid c2) {
			if (this.corr.length != c2.corr.length) {
				return false;
			}
			for (int i = 0; i < corr.length; i++) {
				if (corr[i] != c2.corr[i]) {
					return false;
				}
			}
			return true;

		}

		public double dis(Centroid c2) {
			double result = 0.0;
			if (this.corr.length != c2.corr.length) {
				System.out
						.println("something is wrong with mapping, 9999 distance");
				return 9999;
			}
			for (int i = 0; i < corr.length; i++) {
				result += (c2.corr[i] - this.corr[i])
						* (c2.corr[i] - this.corr[i]);
			}

			return Math.sqrt(result);
		}

	}

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {

		// private final IntWritable ONE = new IntWritable(1);
		/**
		 * 
		 * @param cs
		 * @param c
		 * @return
		 */
		public Centroid nearestCentroid(Centroid[] cs, Centroid c) {
			Centroid result = cs[0];
			double dis = c.dis(result);
			for (int i = 0; i < cs.length; i++) {
				if (c.dis(cs[i]) < dis) {
					dis = c.dis(cs[i]);
					result = cs[i];
				}
			}
			return result;
		}

		public Centroid averageCentroids(String[] cs) {
			Centroid f = new Centroid(cs[0]);
			Double[] avgResult = new Double[f.corr.length];
			for (int i = 0; i < cs.length; i++) {
				Centroid temp = new Centroid(cs[i]);
				for (int j = 0; j < cs[i].length(); j++) {
					avgResult[j] += temp.corr[j];
				}
			}
			for (int i = 0; i < avgResult.length; i++) {
				avgResult[i] /= cs.length;
			}
			return new Centroid(avgResult);

		}

		public Centroid nearestCentroid(ArrayList<Centroid> cs, Centroid c) {
			Centroid result = cs.get(0);
			double dis = c.dis(result);
			for (int i = 0; i < cs.size(); i++) {
				if (c.dis(cs.get(i)) < dis) {
					dis = c.dis(cs.get(i));
					result = cs.get(i);
				}
			}
			return result;
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			URI[] localPaths = context.getCacheFiles();
			// centroids is the list holding all old centroids.
			ArrayList<Centroid> centroids = new ArrayList<Centroid>();
			for (URI uri : localPaths) {
				File usersFile = new File(uri);
				BufferedReader reader = null;
				reader = new BufferedReader(new FileReader(usersFile));
				String line = null;
				while ((line = reader.readLine()) != null) {
					centroids.add(new Centroid(line));
				}

				reader.close();
			}

			// check if the last column is false.
			String[] data = value.toString().split("\\s+");
			Centroid inputData = new Centroid(data);
			Centroid nearest = nearestCentroid(centroids, inputData);
			context.write(new Text(nearest.toString()),
					new Text(inputData.toString()));
		}

	}

	public static class MyCombiner extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text text, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			Double[] avgResult = new Double[text.toString().split(",").length];
			for (Text value : values) {
				count++;
				String temp = value.toString();
				Centroid tempc = new Centroid(temp);
				for (int i = 0; i < avgResult.length; i++) {
					avgResult[i] += tempc.corr[i];
				}
			}
			for (int i = 0; i < avgResult.length; i++) {
				avgResult[i] /= count;
			}
			context.write(text, new Text(StringUtils.join(avgResult, ",") + " "
					+ count));
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text text, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int totalCount = 0;
			Double[] avgResult = new Double[text.toString().split(",").length];
			for (Text value : values) {
				String temp = value.toString();
				String[] temp2 = temp.split(" ");
				int tempCount = Integer.parseInt(temp2[1]);
				totalCount += tempCount;
				Centroid tempc = new Centroid(temp2[0]);
				for (int i = 0; i < avgResult.length; i++) {
					avgResult[i] += tempc.corr[i] * tempCount;
				}
			}
			for (int i = 0; i < avgResult.length; i++) {
				avgResult[i] /= totalCount;
			}

			context.write(new Text(StringUtils.join(avgResult, ",")),
					new Text());
		}
	}

}