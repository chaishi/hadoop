package weibo;

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
import org.bigdata.util.HadoopConfig;

/**
 * 统计不同时间段，微博数量，了解用户微博活动时间
 * @author 雪
 * @time 20150526
 */
public class TimeWeiboNum {
	private static class TimeWeiboNumMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().trim().split("~",3);
			String[] strs1 = strs[1].split(" ");
			String[] strs2 = strs1[1].split(":");
			context.write(new Text(strs2[0]), new IntWritable(1));
		}
		
	}
	
	private static class TimeWeiboNumReducer extends Reducer<Text, IntWritable, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for(IntWritable value:values){
				count += value.get();
			}
			
			//[{"time": "13", "number":123}]
			String outKey = "{" + "\"time\":" + key + ",";
			String outValue = "\"number\":" + count + "},";
			context.write(new Text(outKey), new Text(outValue));
		}
		
	}
	
	public static void getTimeWeiboNum() throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"统计不同时间段，微博数量");
		job.setJarByClass(TimeWeiboNum.class);
		
		job.setMapperClass(TimeWeiboNumMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(TimeWeiboNumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("/weibo/hotTopic_in"));
		FileOutputFormat.setOutputPath(job, new Path("/weibo/TimeWeiboNum_out"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
