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
 * 统计用户总数
 * @author 雪
 * @time 20150522
 */
public class UserNum {
	public static class UserNumMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new Text("person"), new IntWritable(1));
		}
		
	}
	
	public static class UserNumReducer extends Reducer<Text,IntWritable, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			long num = 0;
			for(IntWritable value:values){
				num += value.get();
			}
			context.write(new Text("person"), new Text(num + ""));
		}
	}
	
	public static void getUserNum() throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"统计用户总数");
		job.setJarByClass(UserNum.class);
		
		job.setMapperClass(UserNumMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(UserNumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("/weibo/user_in"));
		FileOutputFormat.setOutputPath(job, new Path("/weibo/userNum_out"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
