package pageRank;

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

public class First {
	private static class FirstMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().trim().split("\t",2);
			context.write(new Text(strs[0]), new Text(strs[1]));
		}
		
	}
	
	private static class FirstReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String p = "";
			String out = "";
			for(Text value:values){
				String str = value.toString();
				if(str.startsWith("a") == true){
					p = str.split("\t")[1];
				}else{
					out = str;
				}
			}
			context.write(key,new Text( p + "#" + out));
		}
		
	}
	
	public static void runFirst() throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"pageRank1");
		job.setJarByClass(First.class);
		
		job.setMapperClass(FirstMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(FirstReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("/pageRank_in_link"));
		FileInputFormat.addInputPath(job,new Path("/pageRank_in_p"));
		FileOutputFormat.setOutputPath(job, new Path("/pageRank_out"));
		
		job.waitForCompletion(true);
	}
}
