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
 * 统计topic热门程度，排序
 * @author 雪
 * @time 20150523
 */
public class HotTopicSort {
	public static class HotTopicSortMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String strs[] = value.toString().split("\t");
			context.write(new IntWritable(Integer.parseInt(strs[1])), new Text(strs[0]));
		}
		
	}
	
	public static class HotTopicSortReducer extends Reducer<IntWritable, Text, Text, Text>{

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String str = "";
			for(Text value:values){
				str += value.toString() + "\t";
			}
			int outKey = key.get();
			
			//[{"topic": "同桌的你", "number": "3197800"}]
			String outKeyStr = "{" + "\"number\":" + "\"" + outKey + "\"" + ",";
			String outNumStr = "\"topic\":" + "\"" + str.trim() + "\"" + "},";
			context.write(new Text(outKeyStr), new Text(outNumStr));
		}
		
	}
	
	public static void getHotTopicSort() throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"统计topic热门程度，排序");
		job.setJarByClass(HotTopicSort.class);
		
		job.setMapperClass(HotTopicSortMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(HotTopicSortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("/weibo/hotTopic_out"));
		FileOutputFormat.setOutputPath(job, new Path("/weibo/hotTopicSort_out"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
