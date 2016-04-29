package grid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GridDriver 
{
	public static void main(String[] args) throws Exception
	{
		// args[0] is the number of splits
		// args[1] is the path to the input data files
		// args[2] is the path to the output files
		
		String numSplits = args[0];
		Configuration conf1 = new Configuration();
		conf1.set("numSplits",numSplits);
		Job job1 = Job.getInstance(conf1, numSplits);
		job1.setJarByClass(GridDriver.class);
		job1.setMapperClass(GridMapper.class);
		//job1.setReducerClass(GridReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
	
}
