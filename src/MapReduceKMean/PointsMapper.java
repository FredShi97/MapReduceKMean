package MapReduceKMean;

import java.io.IOException;
import java.awt.Point;
import java.awt.geom.Point2D;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class PointsMapper extends Mapper<LongWritable, Text, Text, Text> {

    // centroids : Linked-list/arraylike
    private Point2D.Double[] centers; 
    private Text clusterId = new Text(); 

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

        super.setup(context);
        Configuration conf = context.getConfiguration();
        if (conf == null){
            System.err.println("conf is null");
            System.exit(1);
        }   

        if (conf.get("kCentroids") == null){
            System.err.println("kCentroids is null");
            System.exit(1);
        }   


        int k = Integer.parseInt(conf.get("kCentroids"));

        
        

        centers = new Point2D.Double[k];
        for(int i = 0; i < k; i++) {
            String pt= conf.get("centroid." + Integer.toString(i));
            System.err.println(pt);
            String[] xy = pt.split(","); 
            System.err.println("length is " + xy.length); 
            
            System.err.println("x " + xy[0]);
            System.err.println("y " + xy[1]);
            if (centers == null)
                System.err.println("centers is null");
            centers[i] = new Point2D.Double(Double.parseDouble(xy[0]), Double.parseDouble(xy[1]));
        }

    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] pt = value.toString().split(",");
        Point2D.Double newPt = new Point2D.Double(Double.parseDouble(pt[0]), 
        Double.parseDouble(pt[1]));
        double minDist = Double.MAX_VALUE; 
        int clusterIndex = 0; 
        int index = 0; 

        for (Point2D.Double center:centers){
            double dist = Math.abs(Math.pow(center.x - newPt.x, 2) + Math.pow(center.y - newPt.y, 2)); 
            if (dist < minDist){
                clusterIndex = index;
                minDist = dist; 
            }
            index += 1; 
        }

        clusterId.set(Integer.toString(clusterIndex)); 
        context.write(clusterId, value);


    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

}