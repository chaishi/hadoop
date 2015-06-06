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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.util.HadoopConfig;


/**
 * 获取影响力排名
 * @author 雪
 * @time 20150526
 */
public class TopInfluenceMan {
	private static class TopInfluenceManMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, DoubleWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");
			String[] strs1 = strs[1].split("#");
			context.write(new DoubleWritable(Double.parseDouble(strs1[1])), new Text(strs[0] + "#" + strs1[0] + "#" + strs1[2]));
		}
		
	}
	
	private static class TopInfluenceManReducer extends Reducer<DoubleWritable, Text, Text, Text>{

		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> values,
				Reducer<DoubleWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = new String[100];
			for(Text value:values){
				strs = value.toString().split("#");
			}
			String[] strs2 = strs[2].split("&");
			String outKey = "{" + "\"influence\":" + "\"" + key.get() + "\"" + ", ";
			String outValue = "\"userId\":"+ "\"" + strs[0] + "\"" + ", " 
							+ "\"userName\":"+ "\"" + strs[1] + "\"" + ", ";
			
			outValue += "\"relationUsers\":[";
			for(String str:strs2){
				outValue += "\"" + str + "\", ";
			}
			outValue += "] },";
			//[{"influence":"2.3569710000000003E-6", "userId":"123495", "userName":"name", "relation":["2837594","47910348"]}]
			context.write(new Text(outKey), new Text(outValue));
		}
		
	}
	
	public static void getTopInfluenceMan(String inPath) throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"获取影响力排名");
		job.setJarByClass(TopInfluenceMan.class);
		
		job.setMapperClass(TopInfluenceManMapper.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(TopInfluenceManReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path("/weibo/TopInfluenceMan_out"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
