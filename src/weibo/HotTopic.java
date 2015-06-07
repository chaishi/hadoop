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
 * 统计topic热门程度
 * @author 雪
 * @time 20150523
 */
public class HotTopic {
	public static class HotTopicMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String strs[] = value.toString().split("~",9);
			long num = 0;
			num += Long.parseLong(strs[3]);//回复数量
			num += Long.parseLong(strs[4]);//评论数量
			num += Long.parseLong(strs[5]);//点赞数量
			//key = topic ; value = 总数量(即num)
			context.write(new Text(strs[7]), new LongWritable(num));
		}
	}
	
	public static class HotTopicReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long totalNum = 0;
			for(LongWritable value:values){
				totalNum +=  Long.parseLong(value.toString());
			}
			context.write(new Text(key), new LongWritable(totalNum));
		}
		
	}
	
	public static void getHotTopic() throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"统计topic热门程度");
		job.setJarByClass(HotTopic.class);
		
		job.setMapperClass(HotTopicMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(HotTopicReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job,new Path("/weibo/hotTopic_in"));
		FileOutputFormat.setOutputPath(job, new Path("/weibo/hotTopic_out"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
