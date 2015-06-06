package org.bigdata.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class grandParent {
	
	public static class grandParentMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			context.write(new Text(strs[0]), new Text(strs[1] + ":1"));
			context.write(new Text(strs[1]), new Text(strs[0] + ":2"));
		}
	}
	
	public static class grandParentReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			List<String> left = new ArrayList <String>();
			List<String> right = new ArrayList <String>();
			for(Text value:values){
				String[] strs = value.toString().split(":");
				if(strs[1].equals("1")){
					right.add(strs[0]);
				}else{
					left.add(strs[0]);
				}
			}
			for(int i = 0, len = left.size(); i < len; i++){
				for(int j = 0, len1 = right.size(); j < len1; j++){
					context.write(new Text(left.get(i)),new Text(right.get(j)));
				}
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"统计爷爷和孙子");
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(grandParentMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(grandParentReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("/parent_in"));
		FileOutputFormat.setOutputPath(job, new Path("/parent_out/"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
