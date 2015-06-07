package weibo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * 计算最具影响力的人物 之 pageRank算法第二步
 * @author 雪
 * @time 20150522
 */
public class PageRankStep2 {
	public static class PageRankStep2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			Path filePath = fileSplit.getPath();
			String fileName = filePath.getName();
			if(fileName.startsWith("part")){
				String[] strs = value.toString().split("#");
				String[] strs1 = strs[0].split("\t");
				String outKey = strs1[0];
				Double p = Double.parseDouble(strs1[1]);
				context.write(new Text(outKey), new DoubleWritable(0.0));
				//关注的用户
				if(strs.length > 1){
					String[] links = strs[1].split("\t");
					long len = links.length;
					for(String link:links){
						context.write(new Text(link), new DoubleWritable(p / len));
					}
				}
			}
		}
		
	}
	
	public static class PageRankStep2Reducer extends Reducer<Text, DoubleWritable, Text, Text>{
		public static final double A = 0.85;
		
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Reducer<Text, DoubleWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			for(DoubleWritable value:values){
				sum += value.get();
			}
			double p = A * sum + (1 - A) * 0.00001571314; //A为心灵转移参数,sum为本次计算的概率
			context.write(new Text(key), new Text("a\t" + p));
		}
		
	}
	
	public static void runStep2(String inPath,String outPath) throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"pageRankStep2");
		job.setJarByClass(PageRankStep2.class);
		
		job.setMapperClass(PageRankStep2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setReducerClass(PageRankStep2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		
		job.waitForCompletion(true);
	}
}
