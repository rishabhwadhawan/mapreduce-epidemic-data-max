import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class equijoin {
	
	static HashMap<Integer,String> mainhash = new HashMap<Integer,String>();
	
	static class joinMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			List<String> list = new ArrayList<String>();
						
			list.addAll((List<String>)Arrays.asList(line.split("::")));
			
			int primarykey = Integer.parseInt(list.get(1));
			String tuple1 = list.get(2);
			
			if(mainhash.containsKey(primarykey))
			{
				String tuple2 = mainhash.get(primarykey).toString();
				String joinvalue = tuple1+","+tuple2;
				Text newkey = new Text("Join");
				context.write(new Text(newkey), new Text(joinvalue));
			}
		}
	}

	static class joinReducer extends Reducer<Text,Text,Text,Text>
	{	    
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			for(Text val : values)
			{
				context.write(key, val);
			}	
		}
	}

	public static void main(String args[]) throws Exception
	{
		Path pt = new Path("hdfs://localhost:54310/user/hadoop1/Join/second-i/table2.txt");
		
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line;
		while(( line  = reader.readLine())!= null)
		{
			
			if(line.equals(""))
			{
				break;
			}
			
			List<String> mylist = new ArrayList<String>();
			mylist.addAll((List<String>)Arrays.asList(line.split("::")));
			mainhash.put(Integer.parseInt(mylist.get(1)), mylist.get(2));
		}
			
		if(args.length !=2)
		{
			System.out.println("Not enough arguments");
			System.exit(-1);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(equijoin.class);
		job.setMapperClass(joinMapper.class);
		job.setReducerClass(joinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
