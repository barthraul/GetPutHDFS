package getput.hdfs;

import java.io.BufferedInputStream;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSjob 
{
	public static void main (String [] args) throws Exception{
    	String localSrc = null;  	
        String dst = null;  
        
        if(args[0].equals("wrec")){
        	localSrc = args[1]; dst = args[2]; writeHDFSRecursively(localSrc, dst);
        } else if (args[0].equals("wnor")){
        	localSrc = args[1]; dst = args[2]; writeToHDFSFull(localSrc, dst);
        } else if (args[0].equals("r")){
        	localSrc = args[1]; GetHDFS(localSrc);
        }
        else
        	System.out.println("\n Think you missed something... " +
        						"\n 	hadoop jar <project_jar> <type_of_work> <path_1> <path_2>" +
        						"\n 	<type_of_work>: so far the job can execute three different tasks " +
        						"\n 		wrec: get information recursively from folders. In this case the job gets all sub-folders and files of the path given in <path_1>" +
        						"\n 		wnor: get information from 1 file. A file must be passed in <path_1>" +
        						"\n 		r: read messages from a specific HDFS directory. n that case, the HDFS path must be given in <path_1>. No need of <path_2>" +
        						"\n Job not executed... finished. \n");
    }
     
    @SuppressWarnings("restriction")
	public static void writeToHDFSFull(String message, String dst) throws IOException{                       
        //Input stream for the file in local file system to be written to HDFS
        InputStream in = new BufferedInputStream(new FileInputStream(message));

        //Get configuration of Hadoop system
        Configuration conf = new Configuration();
        conf.set("spark.scheduler.allocation.file","/schedulerFile.xml");
        System.out.println("Connecting to -- "+conf.get("fs.defaultFS"));

        //Destination file in HDFS
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst));
                         
        //Copy file from local to HDFS
        IOUtils.copyBytes(in, out, 4096, true);                     
        System.out.println(dst + " copied to HDFS");    
     }
        
     @SuppressWarnings("restriction")
	public static void writeHDFSRecursively(String localSrc, String dst) throws IOException{
        //Input stream for the path in local file system to be written to HDFS
        File dir = new File (localSrc);
        File[] files = dir.listFiles();
     
        //Get configuration of Hadoop system
        Configuration conf = new Configuration();
        conf.set("spark.scheduler.allocation.file","/schedulerFile.xml");
        System.out.println("Connecting to -- "+conf.get("fs.defaultFS"));
            
        //copy all files in the directory
        //just not empty files
        for(File file : files){
            if(!file.getName().contains("crc") && file.length()>0){
            	InputStream in = new BufferedInputStream(new FileInputStream(file));                                        
                //Destination file in HDFS
                FileSystem fs = FileSystem.get(URI.create(dst+file.getName()), conf);
                OutputStream out = fs.create(new Path(dst+file));                
                //Copy file from local to HDFS
                IOUtils.copyBytes(in, out, 4096, true);                     
                System.out.println(dst+file.getName() + " copied to HDFS");     
            }
       }
     }
     
     @SuppressWarnings("restriction")
	public static void GetHDFS(String src) throws IOException{
    	Configuration conf = new Configuration();	 
 		//Get the filesystem - HDFS
 		FileSystem fs = FileSystem.get(URI.create(src), conf);
 		FSDataInputStream in = null;
 			 
 		try {
 		    //Open the path mentioned in HDFS
 			System.out.println("Reading file" + src.toString());
 		    in = fs.open(new Path(src));
 		    IOUtils.copyBytes(in, System.out, 4096, false);	    	 
 		    System.out.println("End Of file: HDFS file read complete"); 	 
 		} finally {
 			IOUtils.closeStream(in);
 		}
     }
}