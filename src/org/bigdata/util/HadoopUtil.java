package org.bigdata.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * ������
 * @author ѩ
 * @date 20150510
 */
public class HadoopUtil {
	//�����ļ���
	public static void mkdir(String dirPath) throws IOException{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		fs.mkdirs(new Path(dirPath));
		fs.close();
	}
	
	//ɾ���ļ�
	public static void delete(String path) throws IOException{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		fs.deleteOnExit(new Path(path));
		fs.close();
	}
	
	//�����ļ�
	public static void createFile(String path) throws IOException{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		fs.create(new Path(path));
		fs.close();
	}
	
	//�����ļ�
	public static void listFile(String path) throws IOException{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		FileStatus[] status = fs.listStatus(new Path(path));
		for(FileStatus fileStatus:status ){
			System.out.println(fileStatus.getPath().toString());
		}
		fs.close();
	}
	
	//�ϴ��ļ�
	public static void upload(String src,String dest) throws IOException{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		fs.copyFromLocalFile(new Path(src), new Path(dest)); 
		fs.close();
	}
	
	//�����ļ�
	
	public static void download(String src,String dest) throws IOException{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		fs.copyToLocalFile(new Path(src), new Path(dest)); 
		fs.close();
	}
	
	//����
	public static void main(String[] args) throws IOException{
		mkdir("/20150514");
		//delete("/your");
		//createFile("/test/hello.txt");
		//listFile("/test");
		//upload("E:/Eclipse_workplace/test.xml","/test/");
		//download("/test/test.xml","E:/Eclipse_workplace/");
	}
}




