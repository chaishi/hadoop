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
 * 因为微博topic 和  用户性别在2个文件中，所以需要先通过userId将用户性别，和topic关联起来
 * @author 雪
 * @time 20150526
 */
public class CombineGenderTopic {
	//输出键值key为userId, 键值value 为  gender 或  topic
	private static class CombineGenderTopicMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			Path filePath = fileSplit.getPath();
			String fileName = filePath.getName();
			String[] strs = value.toString().split("~");
			if(fileName.startsWith("user")){ //读取user文件时，里面有gender和userId的信息
				context.write(new Text(strs[0]), new Text(strs[3]));
			}else{//读取weibo文件时，里面有topic和 userId信息
				context.write(new Text(strs[6]), new Text(strs[7]));
			}
		}
	}
	//根据userId 将gender和topic聚集在一起
	private static class CombineGenderTopicReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] str = new String[3];
			for(Text value:values){
				String gender = value.toString().trim();
				if(gender.equals("f") || gender.equals("m")){
					str[0] = gender;
				}else{
					str[1] = gender;
				}
			}
			context.write(new Text(key), new Text(str[0] + "$" + str[1]));
		}
		
	} 
	
	public static void getCombineGenderTopic() throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"通过userId将用户性别，和topic关联起来");
		job.setJarByClass(CombineGenderTopic.class);
		
		job.setMapperClass(CombineGenderTopicMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(CombineGenderTopicReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("/weibo/user_in"));
		FileInputFormat.addInputPath(job,new Path("/weibo/hotTopic_in"));
		FileOutputFormat.setOutputPath(job, new Path("/weibo/CombineGenderTopic_out"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
