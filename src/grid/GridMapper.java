package grid;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class GridMapper extends Mapper<Object, Text, Text, Text> 
{
	private Text keyString = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		String numSplits = conf.get("numSplits");
		String[] line = value.toString().split("\\p{Space}+");
		
		String location = getGridPosition(line[13],line[14],numSplits);
	
		keyString.set(location);
		context.write(keyString,value);
		
	}
	
	private String getGridPosition(String lat, String lon, String numSplits)
	{
		try{
			// Change latitude, longitude and the number of splits into numbers to work with.
			int latNum = Integer.parseInt(lat);
			int lonNum = Integer.parseInt(lon);
			int splits = Integer.parseInt(numSplits);
			
			// This is the width of the grid. It is 23 latitudes high and 59 longitudes wide.
			int latWidth = 23;
			int lonWidth = 59;
			
			int startLat = 25000;
			int startLon = -126000;
			
			// This is the distance between each split.
			double latSplit = (double)latWidth/(double)splits;
			double lonSplit = (double)lonWidth/(double)splits;
			
			// This is the coordinate of each lat/lon. The distance from the starting point, divided by the size of the split. 
			int latLocation = (int)Math.floor(((double)latNum - startLat)/latSplit/1000);
			int lonLocation = (int)Math.floor(((double)lonNum - startLon)/lonSplit/1000);
			
			// Create a string as a key giving the coordinates.
			String location = latLocation + " " + lonLocation;
			return location;
			
		}catch(NumberFormatException e){
			System.out.println("Could not parse lat/lons");
			return null;
		}
	}
}
