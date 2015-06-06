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
 * 统计微博用户活跃度
 * @author 雪
 * @time 20150524
 */
public class ActivityDegree {
	
	public static class ActivityDegreeMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split("~");
			int activityNum = Integer.parseInt(strs[1]) + Integer.parseInt(strs[2]);
			int i;
			for(i = 0; i < 20000; i += 1500){
				if(activityNum >= i && activityNum < i + 1500){
					context.write(new IntWritable(i), new Text((i + 1500) + "#" +1));
					break;
				}
			}
			if(i >= 20000){
				context.write(new IntWritable(20000), new Text("以上#"+1));
			}
		}
		
	}
	
	public static class ActivityDegreeReducer extends Reducer<IntWritable, Text, Text, Text>{

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int num = 0;
			String stand = "";//范围最高值
			for(Text value:values){
				String[] strs = value.toString().split("#");
				num += Integer.parseInt(strs[1]);
				stand = strs[0];
			}
			//[{"range-low":"0", "range-high":"1500","number":123,"percentage":0.004%},{},{}]
			String outKey = "{" + "\"range_low\":" + key + ", " + "\"range_high\":" + "\"" + stand + "\""  + ", ";
			String outNum = "\"number\":" + num + ", " + "\"percentage\":" + "\"" + (num/63641.0) * 100 + "%"+ "\"" + "},";
			context.write(new Text(outKey), new Text(outNum));
		}
		
	}
	
	public static void getActivityDegree() throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"统计用户活跃度");
		job.setJarByClass(ActivityDegree.class);
		
		job.setMapperClass(ActivityDegreeMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(ActivityDegreeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("/weibo/user_in"));
		FileOutputFormat.setOutputPath(job, new Path("/weibo/activityAgree_out"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
