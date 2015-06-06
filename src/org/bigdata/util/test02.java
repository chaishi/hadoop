package org.bigdata.util;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.GzipCodec;

public class test02 {
	/**
	 * @param args
	 * @throws IOException
	 * @function 解压文件
	 * @author 雪
	 * @date 2015年5月14日
	 */
	public static void main(String[] args) throws IOException{
		Configuration config = HadoopConfig.getConfig();
		Path path = new Path("/hello.gz");
		FileSystem fs = FileSystem.get(config);
		InputStream is = fs.open(path);
		GzipCodec codec = new GzipCodec();
		codec.setConf(config);
		CompressionInputStream cis = codec.createInputStream(is);
		byte[] buffer = new byte[1024];
		int read =-1;
		while((read = cis.read(buffer)) != -1){
			System.out.println(new String(buffer, 0 ,read));
		}
		cis.close();
	}
}
