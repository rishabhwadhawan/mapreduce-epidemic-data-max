import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.lang.Math;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class normalizing_data_using_mr
{
	static class CaluculateMaxFromEachMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
	{
		Text new_key = new Text("US");
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			double max_value = Double.MIN_VALUE;
			String c_line = null; 
			//while((
			c_line = value.toString();
			//{
				List<String> new_value = new ArrayList<String>();
				new_value.addAll((List<String>) Arrays.asList(c_line.split(",")));
				//System.out.println("****************************************" + each_value.size());
				for(int x = 2; x < new_value.size();x++)
				{
					try
					{
						//System.out.println("I am here");
						double number = Double.parseDouble(each_value.get(x));
						if(number > max_value)
						{
							max_value = number;
						}
					}
					catch(NumberFormatException e)
					{
						//System.out.println("Error parsing to double");
					};
				}
			//}
			context.write(new_key, new DoubleWritable(max_value));
		}
	}

	static class FindingMaxFromAllReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{
	public void reduce(Text new_key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException
			{
			double maximum_of_all = Double.MIN_VALUE;
			
			for(DoubleWritable value : values)
			{
				maximum_of_all = Math.max(maximum_of_all, value.get()); 
			}
			context.write(new_key, new DoubleWritable(maximum_of_all));
			}
	}

	public static void main(String args[]) throws Exception

	{
		if(args.length !=2)
		{
			System.out.println("Not enough arguments");
			System.exit(-1);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(normalizing_data_using_mr.class);
		job.setMapperClass(CaluculateMaxFromEachMapper.class);
		job.setReducerClass(FindingMaxFromAllReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
