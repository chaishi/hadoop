package weibo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.util.HadoopConfig;


/**
 * 获取输出user 初始 概率
 * @author 雪
 * @time 20150526
 */
public class InitProbability {
	public static class InitProbabilityMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split("~");
			context.write(new Text(strs[0]), new Text("a\t0.00001571314"));
		}
		
	}
	
	public static class InitProbabilityReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for(Text value : values){
				context.write(new Text(key), new Text(value));
			}
		}
	}
	
	public static void getUserProbability() throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"获取用户初始概率");
		job.setJarByClass(InitProbability.class);
		
		job.setMapperClass(InitProbabilityMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(InitProbabilityReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("/weibo/user_in"));
		FileOutputFormat.setOutputPath(job, new Path("/weibo/userProba_out"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
