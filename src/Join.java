import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Join {
	
	public static class mymapper extends Mapper<LongWritable,Text,Text,Text>
	{
		
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
			String[] parts= value.toString().split(",");
			String tablename=parts[0];
			String joinkey=parts[1];
			String tuple=parts[2];
			con.write(new Text(joinkey),new Text(tablename+":"+tuple));
		}
	}
	
	public static class myreducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> values,Context con) throws IOException, InterruptedException
		{
			int flag = 32;
			String result=null;
			for(Text t1:values)
			{
				flag = 32;
				String[] value=t1.toString().split(":");
				String x1=value[0];
				String x2=value[1];
				
				for(Text t2:values )
				{
					
					String[] val=t2.toString().split(":");
					String y1=val[0];
					String y2=val[1];
					if(!(x1.equals(y1)))
					{
						result=x2+y2;
						flag=33;
						break;
					}
					if(flag==33)
						break;
				}
				con.write(key,new Text(result));
			}
		}
	}


	public static void main(String[] args) throws Exception 
	{
		if(args.length !=2)
		{
			System.out.println("ERROR NOTIFICATION");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(Join.class);
		job.setMapperClass(mymapper.class);
		job.setReducerClass(myreducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
	
}

	