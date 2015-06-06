package org.bigdata.util;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

/**
 * 
 * @author 雪
 *
 */
public class test01 {
	/**
	 * @param args
	 * @throws IOException
	 * @function 压缩文件
	 * @author 雪
	 * @date 2015年5月14日
	 */
	public static void main(String[] args) throws IOException{
		Configuration config  = HadoopConfig.getConfig();
		Path path = new Path("/hello.gz");
		FileSystem fs = FileSystem.get(config);
		OutputStream os = fs.create(path);
		CompressionCodec codec = new GzipCodec();
		CompressionOutputStream cos = codec.createOutputStream(os);
		cos.write("hello world!".getBytes());
		cos.close();
	}
}
