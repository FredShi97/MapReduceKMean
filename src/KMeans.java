import MapReduceKMean.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.awt.geom.Point2D;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hdfs.DistributedFileSystem; 


public class KMeans {


	private static Point2D.Double[] oldCentroids; 
	private static Point2D.Double[] newCentroids; 


	private static int countFileLines(BufferedReader br) throws IOException{
		
        // read pts and write randomly initialized centriods to configuration 
		int lineNumber = 0;
		String line = br.readLine(); 
		
		
        while ( line != null) {
            if (lineNumber + 1 == Integer.MAX_VALUE)
				lineNumber = Integer.MAX_VALUE;
			else
				lineNumber += 1;
			line = br.readLine(); 
        }
        br.close();
		return lineNumber;

	}

	private static void initializeCentroids(Configuration conf) throws IOException{

		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		String uri = conf.get("readInputPath"); 
		Path path = new Path(uri);
    	FileSystem fs = FileSystem.get(URI.create(uri), conf);
    	FSDataInputStream inputStream = fs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

		//read to check line number 
		int fileSize = countFileLines(br); 
		
		int k = Integer.parseInt(conf.get("kCentroids"));


		List<Integer> randStart = new ArrayList<Integer>();
        Random random = new Random();
        int pos = 0;
        while(randStart.size() < k) {
            pos = random.nextInt(fileSize);
            if(pos >= 0 && pos < fileSize && !randStart.contains(pos)) {
                randStart.add(pos);
            }
        }

		Collections.sort(randStart); 

		//Read it one more time to get randomly selected value, open stream again
		inputStream = fs.open(path);
		br = new BufferedReader(new InputStreamReader(inputStream));
        // read pts and write randomly initialized centriods to configuration 
		int index = 0;
		int lineNumber = 0;  
		String line = br.readLine(); 
		
        while ( line != null && index < k) {
			if (lineNumber == randStart.get(index)){
				conf.set("centroid." + Integer.toString(index), line); 
				index += 1;
			}
            	
			lineNumber += 1;
			line = br.readLine(); 
        }
        br.close();


	}

	private static Boolean checkConvergence(Double tolerance){
		
		for (int i = 0; i < oldCentroids.length; i++){
			if (Math.abs(oldCentroids[i].x - newCentroids[i].x) >= tolerance
				|| Math.abs(oldCentroids[i].y - newCentroids[i].y) >= tolerance)
				return false; 
		}

		return true; 
	}


	private static void readClusterResults(Configuration conf, int itr) throws IOException{
		
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		String uri = conf.get("readOutputPath"); 
		Path path = new Path(uri);
    	FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FileStatus[] status = fs.listStatus(path);	
		int index = 0; 

		//Move new centriod to old centriod 
		for (int i = 0; i < newCentroids.length; i++){
			oldCentroids[i].x = newCentroids[i].x; 
			oldCentroids[i].y = newCentroids[i].y; 
		}


		for (int i = 0; i < status.length; i++) {
            //Read the centroids from the hdfs
            //if(!status[i].getPath().toString().endsWith("_SUCCESS")) {
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
			String line = br.readLine(); 
			while ( line != null){
				line = line.replace(","," ");
				//First index is key, second index is value.x, third is value.y
				String[] keyValues = line.split("\\s+"); 
				newCentroids[index].x = Double.parseDouble(keyValues[1]);
				newCentroids[index].y = Double.parseDouble(keyValues[2]);
				line = br.readLine(); 
				index += 1; 
			}
			br.close();
        }

		fs.rename(new Path(uri), new Path(uri + "_iteration_" + Integer.toString(itr))); 

	}




	public static void main(String[] args) throws Exception {

		
		if (args.length != 4){
			System.err.println("Usage: <k> <hdfs://location:port> <input> <output>");
			System.exit(1);
		}
		

		final String kCentriods = args[0];
		final String HDFS_ROOT_URL= args[1];
		final String inputPath = args[2];
		final String outputPath = args[3];
		
		Configuration conf = new Configuration();
		conf.set("kCentroids", kCentriods); 
		//read path add <hdfs://localHost:9000> specified by user 
		conf.set("readInputPath", HDFS_ROOT_URL + inputPath);
		conf.set("readOutputPath", HDFS_ROOT_URL + outputPath);
		conf.set("inputPath", inputPath);
		conf.set("outputPath", outputPath); 


		//Initialize centriods
		int k = Integer.parseInt(conf.get("kCentroids"));
		oldCentroids = new Point2D.Double[k]; 
		newCentroids = new Point2D.Double[k];
		for(int i = 0; i < newCentroids.length; i++){
			oldCentroids[i] = new Point2D.Double();
			newCentroids[i] = new Point2D.Double();
		} 


		//initialize centroids and add to config 
		initializeCentroids(conf);

		Double tolerance = 0.001; 
		int itr = 0; 
		final int itrMax = 2; 
		Boolean converged = false; 

		while (itr < itrMax || ! converged) {

			
			Job job = Job.getInstance(conf, "iter_" + itr);
            job.setJarByClass(KMeans.class);
            job.setMapperClass(PointsMapper.class);
            job.setCombinerClass(PointsReducer.class);
            job.setReducerClass(PointsReducer.class);        
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));


			job.waitForCompletion(true);


			readClusterResults(conf, itr); 
			converged = checkConvergence(tolerance); 
			itr += 1; 

		}

		System.out.println("Iteration finished at iter:" + Integer.toString(itr) + " Converged: " + Boolean.toString(converged));
		for (int i = 0; i < newCentroids.length; i++){
			System.out.println("Centriod." + Integer.toString(i) + " x: " + Double.toString(newCentroids[i].x) 
			+ " y: " + Double.toString(newCentroids[i].y) );
		}
		

	}

}