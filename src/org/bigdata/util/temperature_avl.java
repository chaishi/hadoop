package org.bigdata.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class temperature_avl {
	private static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String strs = value.toString();
			int highTemp = Integer.parseInt(strs.substring(13,19).trim());
			int lowTemp = Integer.parseInt(strs.substring(19,25).trim());
			int temp = 0;
			if(highTemp != -9999){
				temp += highTemp;
			}
			if(lowTemp != -9999){
				temp += lowTemp;
			}
			if(temp != 0)
				context.write(new Text(strs.substring(0,4)),new IntWritable(temp/2));
		}
	}
	
	private static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int count = 0;//温度数量
			int temp = 0;//总温度
			for(IntWritable value:values){
				count++;
				temp += value.get();
			}
			context.write(key,new IntWritable(temp/count));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"统计每年平均温度");
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(WordMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(WordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//FileInputFormat.addInputPath(job,new Path("/luoxue_in"));
		//FileOutputFormat.setOutputPath(job, new Path("/luoxue_outAverage/"));
		
		FileInputFormat.addInputPath(job,new Path("/tmpt"));
		FileOutputFormat.setOutputPath(job, new Path("/tmpt_outAverage"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
