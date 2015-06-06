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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.util.HadoopConfig;
/**
 * 根据UserId将用户名，用户影响力，以及用户关注的其他用户放在一起
 * @author 雪
 * @time 20150526
 */

public class UserNameInfluence {
	private static class UserNameInfluenceMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			Path filePath = fileSplit.getPath();
			String fileName = filePath.getName();
			String[] strs = null;
			if(fileName.startsWith("user")){
				strs = value.toString().trim().split("~");
				context.write(new Text(strs[0]), new Text(strs[strs.length - 1] + ":username"));
			}else if(fileName.startsWith("influence")){
				strs = value.toString().trim().split("\t");
				context.write(new Text(strs[0]), new Text(strs[strs.length - 1] + ":influence"));
			}else{//"relation"
				strs = value.toString().trim().split("\t",2);
				context.write(new Text(strs[0]), new Text(strs[1] + ":relation"));
			}
		}
		
	}
	
	private static class UserNameInfluenceReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] str = new String[4];
			for(Text value:values){
				String[] val = value.toString().trim().split(":");
				if(val[1].equals("influence")){
					str[1] = val[0];
				}else if(val[1].equals("relation")){
					String[] relation = val[0].trim().split("\t");
					str[2] = "";
					for(String rel:relation){
						if(rel != null)
							str[2] += rel + "&";
					}
				}else if(val[1].equals("username")){
					str[0] = val[0];
				}
			}
			context.write(new Text(key) , new Text(str[0] + "#" + str[1] + "#" + str[2]));
		}
		
	}
	
	public static void getUserNameInfluence() throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"根据UserId将用户名，用户影响力，以及用户关注的其他用户放在一起");
		job.setJarByClass(UserNameInfluence.class);
		
		job.setMapperClass(UserNameInfluenceMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(UserNameInfluenceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("/weibo/influence_in"));
		FileInputFormat.addInputPath(job,new Path("/weibo/user_in"));
		FileInputFormat.addInputPath(job,new Path("/weibo/relation_out"));
		FileOutputFormat.setOutputPath(job, new Path("/weibo/UserNameInfluence_out"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
