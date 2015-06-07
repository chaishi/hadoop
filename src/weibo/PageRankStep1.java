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
 * 计算最具影响力的人物 之 pageRank算法第一步
 * @author 雪
 * @time 20150522
 */
public class PageRankStep1 {
	public static class PageRankStep1Mapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().trim().split("\t",2);
			context.write(new Text(strs[0]), new Text(strs[1]));
		}
	}
	
	public static class PageRankStep1Reducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String p = "",out = "";
			for(Text value:values){
				String str = value.toString();
				if(str.startsWith("a") == true){
					p = str.split("\t")[1];
				}else{
					out = str;
				}
			}
			//1000562531	0.00001571314#1691761292	2093879035
			context.write(new Text(key), new Text(p + "#" + out));
		}
		
	}
	
	public static void runStep1(String inPath,String outPath) throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"pageRank1");
		job.setJarByClass(PageRankStep1.class);
		
		job.setMapperClass(PageRankStep1Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(PageRankStep1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("/weibo/relation_out"));
		FileInputFormat.addInputPath(job,new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		
		job.waitForCompletion(true);
	}
}
