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
 * 统计各个城市的微博用户数量
 * @author 雪
 * @time 20150526
 */
public class CityUserNum {
	public static class CityUserNumMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split("~");
			context.write(new Text(strs[6].split(" ")[0]), new IntWritable(1));
		}
		
	}
	
	public static class CityUserNumReducer extends Reducer<Text, IntWritable, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for(IntWritable value:values){
				count += value.get();
			}
			//[{"cityNum":"1","userCount":"234"}]
			String outKey = "{" + "\"province\":" + "\"" + key + "\"" + ",";
			String outValue = "\"userCount\":" + "\"" + count + "\"" + "},";
			context.write(new Text(outKey), new Text(outValue));
		}
		
	}
	
	public static void getCityUserNum() throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"统计各个城市的微博用户数量");
		job.setJarByClass(CityUserNum.class);
		
		job.setMapperClass(CityUserNumMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(CityUserNumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("/weibo/user_in"));
		FileOutputFormat.setOutputPath(job, new Path("/weibo/cityUserNum_out"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
