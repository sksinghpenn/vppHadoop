
//This is a generic map reduce outline class provided as part of the 
//Virtual Pair Programmers Hadoop For Java Developers training course.
//(www.virtualpairprogrammers.com). You may freely use this file for your
//own development.

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class GenericMapReduceToolRunner extends Configured implements Tool {

	public static class MapClass extends Mapper<Text, Text,Text, Text > {
		
		public void map (Text key, Text value, Context context)
				throws IOException, InterruptedException {
			//do map processing - write out the new key and value
			context.write(new Text(""),new Text(""));
			
		}
	}
	
	public static class Reduce extends Reducer<Text, Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//do reduce processing - write out the new key and value
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
				//do something
			}
			context.write(new Text(""), new Text(""));
			
		}
	}
	

	public static void deletePreviousOutput(Configuration conf, Path path)  {

		try {
			FileSystem hdfs = FileSystem.get(conf);
			hdfs.delete(path,true);
		}
		catch (IOException e) {
			//ignore any exceptions
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        
        Configuration conf = this.getConf(); 
		deletePreviousOutput(conf, out);
        
		//set any configuration params here. eg to say that the key and value are comma
		//separated in the input data add:
		//conf.set  ("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        
        //this can be overridden at runtime by doing (for example):
        //-D mapreduce.input.keyvaluelinerecordreader.key.value.separator=& 
        
		Job job = Job.getInstance(conf);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MapClass.class);

		//uncomment the following line if specifying a combiner 
		//job.setCombinerClass(Reduce.class);
		
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJarByClass(GenericMapReduceToolRunner.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new GenericMapReduceToolRunner(), args);
		System.exit(result);
	}


	

	

}
