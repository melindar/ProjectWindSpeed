package windspeed;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


@SuppressWarnings("deprecation")
public class WindSpeedDriver 
{
	public static void main(String[] args) throws Exception
	{
		// args[0] is path to lookup file ("stationdata.prn")
		// args[1] is the path to the input data files
		// args[2] is the path to the output files
		
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1);
		//job1.addCacheFile(new URI(args[0]));
		Path lookup = new Path("hdfs://boston:30901/lookup");
		DistributedCache.addCacheFile(lookup.toUri(), job1.getConfiguration());
		job1.setJarByClass(WindSpeedDriver.class);
		job1.setMapperClass(WindSpeedMapper.class);
		//job1.setReducerClass(WindSpeedReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
	
}
