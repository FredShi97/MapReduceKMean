package MapReduceKMean;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PointsReducer extends Reducer<Text, Text, Text, Text> {

    final private Text centriodText = new Text("centriod");
    private Text centriodValue = new Text(); 


    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Input: key -> centroid id/centroid , value -> list of points
        // calculate the new centroid
        // new_centroids.add() (store updated cetroid in a variable)
        Double xSum = 0.0;
        Double ySum = 0.0; 
        int totalPts = 0; 

        for (Text value : values) {
            String[] pt = value.toString().split(",");
            xSum += Double.parseDouble(pt[0]);
            ySum += Double.parseDouble(pt[1]); 
            totalPts += 1; 
        }

        centriodValue.set(Double.toString(xSum / totalPts) + ", " + Double.toString(ySum / totalPts)); 
        context.write(centriodText, centriodValue);


    }


}
