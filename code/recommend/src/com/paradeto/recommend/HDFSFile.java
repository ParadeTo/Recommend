package com.paradeto.recommend;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;

/**
 * HDFS文件操作
 * @author youxingzhi
 *
 */
public class HDFSFile {
	Configuration conf = new Configuration();
	private FileSystem hdfs;
	/**
	 * 构造方法
	 * @param hdfsPath
	 * @throws IOException
	 */
	public HDFSFile(Path hdfsPath) throws IOException{
		hdfs = hdfsPath.getFileSystem(conf);
	}
	/**
	 * 创建目录
	 * @param path
	 * @throws IOException
	 */
	public void mkDir(Path path) throws IOException{		
		hdfs.mkdirs(path);
	}
	/**
	 * 上传文件
	 * @param src
	 * @param dst
	 * @throws IOException
	 */
	public void copyLocalToHdfs(Path src,Path dst) throws IOException{
		hdfs.copyFromLocalFile(src, dst);
	}
	/**
	 * 删除文件
	 * @param path
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public void delFile(Path path) throws IOException{
		hdfs.delete(path);
	}
	/**
	 * 读取文件内容
	 * @param path
	 * @throws IOException
	 */
	public void readFile(Path path) throws IOException{
		//获取文件信息
		FileStatus filestatus = hdfs.getFileStatus(path);
		//FS的输入流
		FSDataInputStream in = hdfs.open(path);
		//用Hadoop的IOUtils工具方法来让这个文件的指定字节复制到标准输出流上
		IOUtils.copyBytes(in,System.out,(int)filestatus.getLen(),false);
		System.out.println();
	}
	/**
	 * 得到文件的修改时间
	 * @param path
	 * @throws IOException
	 */
	public void getModifyTime(Path path) throws IOException{
		FileStatus files[] = hdfs.listStatus(path);
		for(FileStatus file: files){
			//time = file.getModificationTime().
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			String date = sdf.format(file.getModificationTime());
			System.out.println(file.getPath()+"\t"+date);
		}
	}
	/**
	 * 在hdfs上创建文件并写入内容
	 * @param path
	 * @param content
	 * @throws IOException
	 */
	public void writeFile(Path path,String content) throws IOException{
		FSDataOutputStream os = hdfs.create(path);
		//以utf-8的格式写入文件
		os.write(content.getBytes("UTF-8"));
		os.close();
	}
	/**
	 * 列出某一路径下所有的文件
	 * @param path
	 * @throws IOException
	 */
	public void listFiles(Path path) throws IOException{
		hdfs = path.getFileSystem(conf);
		FileStatus files[] = hdfs.listStatus(path);
		int listlength=files.length;  
        for (int i=0 ;i<listlength ;i++){  
            if (files[i].isDir() == false) {  
                System.out.println("filename:"  
                		+ files[i].getPath()  + "\tsize:"  
                        + files[i].getLen());  
            } else {  
                Path newpath = new Path(files[i].getPath().toString());  
                listFiles(newpath);  
            }  
        }  
	}
	public static void main (String arg[]) throws Exception{
		//
		HDFSFile file = new HDFSFile(new Path("hdfs://master:9000/test/"));
		Path dst = new Path("hdfs://master:9000/test/");
		Path src = new Path("/home/youxingzhi/hello.txt");
		//创建目录
		//file.mkDir(dst);
		//上传本地文件到Hdfs
		//file.copyLocalToHdfs(src, dst);
		//读取文件
		//file.readFile(new Path(dst+"/test.txt"));
		//获得修改时间
		//file.getModifyTime(new Path(dst+"/hello.txt"));
		//删除文件或目录
		//file.myDelFile(dst);
		//新建文件，并写入
		//file.writeFile(new Path(dst+"/test.txt"), "我要去深圳");
		//读取目录下所有文件
		//file.listFiles(new Path("hdfs://master:9000/"));
	}
}
