package org.bigdata.util;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author 雪
 * 矩阵相乘
 */
public class matrixArray {
	
	public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		public static int rowM;
		public static int columnM;
		public static int columnN;
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration config = HadoopConfig.getConfig();
			rowM = config.getInt("rowM", 0 );
			columnN = config.getInt("columnN",0);
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(",");
			String i = strs[0];
			String[] strs1 = strs[1].split("\t");
			String j = strs1[0];
			int val = Integer.parseInt(strs1[1]);
			
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			Path filePath = fileSplit.getPath();
			String fileName = filePath.getName();
			if(fileName.startsWith("M")){
				for(int count = 1; count <= columnN; count++){
					context.write(new Text(i + "," + count), new Text("M,"+ j +"," + val));
					//System.out.println(i + "," + count + "----->" +"M,"+ j +"," + val);
				}
			}else{
				for(int count = 1; count <= rowM; count++){
					context.write(new Text(count + "," + j), new Text("N,"+ i +"," + val));
					//System.out.println(count + "," + j +"----->"+"N,"+ i +"," + val);
				}
			}
		}
	}
	
	public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable>{

		public static int columnM;
		
		@Override
		protected void setup(
				Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration config = HadoopConfig.getConfig();
			columnM = config.getInt("columnM", 0);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			int finalVal = 0;
			int[] mArray = new int[columnM+1];
			int[] nArray = new int[columnM+1];
			
			for(Text value:values){
				String[] strs = value.toString().split(",");
				if(strs[0].equals("M")){
					mArray[Integer.parseInt(strs[1])] = Integer.parseInt(strs[2]);
				}else{
					nArray[Integer.parseInt(strs[1])] = Integer.parseInt(strs[2]);
				}
			}
			
			for(int i = 1; i <=columnM; i++ ){
				finalVal += mArray[i] * nArray[i];
			}
			
			context.write(key,new IntWritable( finalVal ));
		}
	}
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		Configuration config = HadoopConfig.getConfig();
		config.setInt("rowM", 3);
		config.setInt("columnM",2 );
		config.setInt("columnN", 4);
		
		Job job = Job.getInstance(config,"矩阵相乘");
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(MatrixMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MatrixReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job,new Path("/matrixArray_in"));
		FileOutputFormat.setOutputPath(job, new Path("/matrixArray_out"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
