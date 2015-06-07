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
import org.bigdata.util.WordCount;

/**
 * 获取微博用户关注关系
 * @author 雪
 * @time 20150523
 */
public class RelationMR {
	public static class RelationMRMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(",");
			//key = userId ; value = 关注的userId 
			context.write(new Text(strs[0]), new Text(strs[1]));
		}
		
	}
	
	public static class RelationMRReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String str = "";
			for(Text value:values ){
				str += value + "\t";
			}
			context.write(new Text(key), new Text(str));
		}
		
	}
	
	public static void getRelationData() throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config," 获取微博任务关注关系");
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(RelationMRMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(RelationMRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("/weibo/relation_in"));
		FileOutputFormat.setOutputPath(job, new Path("/weibo/relation_out"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
