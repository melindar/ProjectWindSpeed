package windspeed;
import java.io.IOException;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


@SuppressWarnings("deprecation")
public class WindSpeedMapper extends Mapper<Object, Text, Text, Text> 
{
	private Text keyString = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		//URI[] localPaths = context.getCacheFiles();
		Path[] localPaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		
		LineData data = new LineData();
		String[] val = data.processLine(value.toString(), localPaths[0]);
		if(val != null){
			String valString = "";
			for(int i = 1; i < val.length; i++){
				valString += " " + val[i];
			}
			if(isInUS(val[13],val[14])){
				keyString.set(val[0]);
				Text valText = new Text(valString);
				context.write(keyString,valText);
			}
		}
		
	}
	
	private boolean isInUS(String lat, String lon)
	{
		try{
			int latNum = Integer.parseInt(lat);
			int lonNum = Integer.parseInt(lon);
			
			if(latNum <= 48000 && latNum >= 25000){
				if(lonNum <= -67000 && lonNum >= -126000)
					return true;
			}
			return false;
			
		}catch(NumberFormatException e){
			System.out.println("Could not parse lat/lons");
			return false;
		}
	}
}
