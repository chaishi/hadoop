package inverse;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.bigdata.util.HadoopConfig;


public class invertIndex {
	
	public static class FileNameInputFormat extends FileInputFormat<Text, Text>{

		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit arg0,
				TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			FileNameRecordReader fnrr = new FileNameRecordReader(); 
			fnrr.initialize(arg0, arg1); 
			return fnrr;
		}
		
	}
	
	public static class FileNameRecordReader extends RecordReader<Text, Text>{
		String fileName;
		LineRecordReader lrr = new LineRecordReader(); 
		@Override
		public void close() throws IOException {
			lrr.close();
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return new Text(fileName);
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return lrr.getCurrentValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return lrr.getProgress();
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1)
				throws IOException, InterruptedException {
			lrr.initialize(arg0, arg1);
			fileName =( (FileSplit)arg0 ).getPath().getName();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return lrr.nextKeyValue();
		}
		
	}
	
	private static class InvertIndexMapper extends Mapper<Text, Text, Text, IntWritable>{
		@Override
		protected void map(Text key, Text value,
				Mapper<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			context.write(new Text(strs[0] + "#" + key.toString()), new IntWritable(1));
		}
	}
	
	private static class InvertIndexCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value: values){
				sum += value.get();
			}
			context.write(new Text(key),new IntWritable(sum));
		}
		
	}

	private static class InvertIndexPartioner extends HashPartitioner<Text, IntWritable>{
		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			String[] strs = key.toString().split("#");
			return super.getPartition(new Text(strs[0]), value, numReduceTasks);
		}
	}

	private static class InvertIndexReducer extends Reducer<Text, IntWritable, Text, Text>{

		static Map<String,String> outputs = new HashMap<String,String>();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = key.toString().split("#");
			String word = strs[0];
			String doc = strs[1];
			int sum = 0;
			for(IntWritable value:values){
				sum += value.get();
			}
			if(outputs.get(word) == null){
				outputs.put(word, "(" + doc + "," + sum + ")");
			}else{
				outputs.put(word,outputs.get(word) + "(" + doc + "," + sum + ")");
			}
		}

		@Override
		protected void cleanup(
				Reducer<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for(String key : outputs.keySet()){
				context.write(new Text(key), new Text(outputs.get(key)));
			}
		}
		
	}

	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"倒序索引");
		job.setJarByClass(invertIndex.class);
		job.setInputFormatClass(FileNameInputFormat.class); 
		job.setMapperClass(InvertIndexMapper.class);
		job.setCombinerClass(InvertIndexCombiner.class);
		job.setReducerClass(InvertIndexReducer.class);
		job.setPartitionerClass(InvertIndexPartioner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path("/InverIndex_in"));
		FileOutputFormat.setOutputPath(job, new Path("/InverIndex_out"));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
