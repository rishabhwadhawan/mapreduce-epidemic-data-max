import java.io.IOException;
import java.util.*;
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

public class epidemic_main_maximum
{
	static class MaxFromEachMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
	{
		static List<String> key_list = new ArrayList<String>();
		HashMap<Object,Object> hmap = new HashMap<Object,Object>();
		HashMap<Object,Object>hash = new HashMap<Object,Object>();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			Double Number;
			Double max_value = Double.MIN_VALUE;
			String c_line = value.toString();
			String k_value = key.toString();
			
			
			if(Integer.parseInt(k_value) == 0)
			{
				key_list.addAll((List<String>) Arrays.asList(c_line.split(",")));
				key_list.remove(1);
				key_list.remove(0);
				return;
			}
			
			List<Double> new_value_list = new ArrayList<Double>();
			List<String> value_list = new ArrayList<String>();

			value_list.addAll((List<String>) Arrays.asList(c_line.split(",")));
			
			  value_list.remove(1);
			  value_list.remove(0);
			
			
			for(int y =0; y<value_list.size();y++)
			{
			
				new_value_list.add(Double.parseDouble(value_list.get(y)));
			}
			
			
			for(int x = 0;x < key_list.size();x++)
			{
				hmap.put(key_list.get(x),new_value_list.get(x));
			}
				
			List<Double> num_list = new ArrayList<Double>();
			
			for(int z =0;z<hmap.size();z++)
			{
				num_list.add((Double)hmap.get((key_list.get(z))));
				for(int p =0;p<num_list.size();p++)
				{
					Number = num_list.get(p);
					if(Number>max_value)
					{
						max_value = Number;
					}
				}
				hash.put(key_list.get(z), max_value);
			}	
			Set set = hash.entrySet();
			Iterator i = set.iterator();
		
			while(i.hasNext())
			{
				Map.Entry me = ((Map.Entry)i.next());
				String map_output_key = me.getKey().toString();
				String map_output_value_string = me.getValue().toString();
				Double map_output_value = Double.parseDouble(map_output_value_string);
				context.write(new Text(map_output_key), new DoubleWritable(map_output_value));	
			}
			
		}
	}

	static class MaxFromAllReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException
			{
				double maximum_of_all = Double.MIN_VALUE;
			
			for(DoubleWritable value : values)
			{
				maximum_of_all = Math.max(maximum_of_all, value.get()); 
			}
				context.write(key, new DoubleWritable(maximum_of_all));
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
		job.setJarByClass(epidemic_main_maximum.class);
		job.setMapperClass(MaxFromEachMapper.class);
		job.setReducerClass(MaxFromAllReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}