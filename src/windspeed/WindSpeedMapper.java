package windspeed;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WindSpeedMapper extends Mapper<NullWritable, Text, Text, Text> 
{
	private Text keyString = new Text();

	public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		URI[] localPaths = context.getCacheFiles();
		String location = conf.get("location");
		
		LineData data = new LineData();
		String[] val = data.processLine(value.toString(), localPaths[0]);
		String valString = "";
		for(int i = 0; i < val.length; i++){
			valString += val[i];
		}
		keyString.set(location);
		Text valText = new Text(valString);
		context.write(keyString,valText);
	}
}
