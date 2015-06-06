package k_means;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.bigdata.util.HadoopConfig;

/**
 * 寻找簇点
 * @author 雪
 */
public class ClusterMR {
	
	public static class ClusterMRMapper extends Mapper<LongWritable, Text, Text, Vector>{

		private static  List<Cluster> clusters = new ArrayList<Cluster>();
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Vector>.Context context)
				throws IOException, InterruptedException {
			clusters.clear();
			Configuration config = context.getConfiguration();
			String readClusterPath = config.get("KNN_in");
			FileSystem fs = FileSystem.get(config);
			FileStatus[] files = fs.listStatus(new Path(readClusterPath));
			if(files != null){
				for(int i = 0,len = files.length; i < len; i++){
					System.out.println(files[i].getPath().getName());
					if(files[i].getPath().getName().startsWith("p")){
						FSDataInputStream fis = fs.open(files[i].getPath());
						LineReader reader = new LineReader(fis,config);
						Text value = new Text();
						while( reader.readLine(value) >0 ){
							Cluster cluster = new Cluster(value.toString());
							clusters.add(cluster);
						}
						System.out.println("初始化簇心");
						for(Cluster cluster : clusters){
							System.out.println(cluster);
						}
						reader.close();
					}
				}
			}
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Vector>.Context context)
				throws IOException, InterruptedException {
			Vector vector = new Vector(value.toString());
			String id = getNearest(vector);
			context.write(new Text(id),vector);
		}
		
		private  String getNearest(Vector vector) {
			double min = Double.MAX_VALUE;
			String id ="";
			for(Cluster cluster : clusters){
				double distance = new EuclideanDistance().getDistance(vector, cluster.getVector());
				if(distance < min){
					min = distance;
					id = cluster.getId();
				}
			}
			return id;
		}

	}
	
	public static class ClusterMRReducer extends Reducer<Text, Vector, Text, NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<Vector> values,
				Reducer<Text, Vector, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			Vector vector = new Vector();
			for(Vector v : values){
				if(sum ==0){
					for(Double value : v.getValues()){
						vector.getValues().add(value);
					}
				}else{
					vector.add(v);
				}
				sum++;
			}
			vector.divide(sum);
			context.write(new Text(key.toString()+","+sum+","+vector.toString()),NullWritable.get());
		}
		
	}
	
	public static void runKMeansJob(String readClusterPath , String writeClusterPath) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		config.set("readClusterPath",readClusterPath);
		Job job = Job.getInstance(config,"KMeans");
		job.setJarByClass(ClusterMR.class);
		job.setMapperClass(ClusterMRMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Vector.class);
		
		job.setReducerClass(ClusterMRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path("/data/input.txt"));
		FileOutputFormat.setOutputPath(job,new Path(writeClusterPath));
		job.waitForCompletion(true);
	}
}
