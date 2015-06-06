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
 * 各个专题男女比例
 * @author 雪
 * @time 20150526
 */
public class GenderPercentage {
	private static class GenderPercentageMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");
			System.out.println(strs[1]);
			String[] strs1 = strs[1].split("[$]");
			System.out.println(strs1[1]);
			context.write(new Text(strs1[1]), new Text(strs1[0]));
		}
		
	}
	
	private static class GenderPercentageReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int maleNum = 0;
			int femaleNum = 0;
			for(Text value:values){
				String str = value.toString().trim();
				if(str.equals("f")){
					femaleNum++;
				}else{
					maleNum++;
				}
			}
			
			String outKey = "{" + "\"topic\":" + "\"" + key + "\"" + ", ";
			String outValue = "\"男\":" + maleNum + ", " + "\"女\":" + femaleNum + "},";
			//[{"topic":"火箭","男":234,"女":34},{}]
			context.write(new Text(outKey), new Text(outValue));
		}
		
	}
	
	public static void getGenderPercentage() throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"获取某topic微博数男女比例");
		job.setJarByClass(GenderPercentage.class);
		
		job.setMapperClass(GenderPercentageMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(GenderPercentageReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("/weibo/CombineGenderTopic_out"));
		FileOutputFormat.setOutputPath(job, new Path("/weibo/GenderPercentage_out"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
