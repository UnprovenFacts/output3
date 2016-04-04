//package org.myorg;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordGroup {

	public static class customMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] arr = line.trim().split(" ");
			
			for(int i=0; i < arr.length; i++){
				if(arr[i].replaceAll("[^A-Za-z]", "").isEmpty() == true)
					continue;

				else {	
					for(int j=0; j<arr.length; j++){
	
						arr[j] = arr[j].replaceAll("[^A-Za-z]", "").toLowerCase();
						
						if(arr[i].equals(arr[j]))
							continue;
						else{
							if(arr[j].isEmpty() == false){
								String temp = arr[i] + " " + arr[j];
								word.set(temp);
								output.collect(word, one);
							}
						}
					}
				}
			}
		}
	}

	public static class customReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WordGroup.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(customMap.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(customReduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
