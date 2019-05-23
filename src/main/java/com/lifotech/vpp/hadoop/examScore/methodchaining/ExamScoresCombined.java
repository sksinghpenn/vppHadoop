package com.lifotech.vpp.hadoop.examScore.methodchaining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;






public class ExamScoresCombined {

	public static void deletePreviousOutput(Configuration conf, Path path)  {

		try {
			FileSystem hdfs = FileSystem.get(conf);
			hdfs.delete(path,true);
		}
		catch (IOException e) {
			//ignore any exceptions
		}
	}

	public static void main(String[] args) throws Exception {
		Path in = new Path(args[0]);
		Path out1 = new Path(args[1]);
		Path out2 = new Path(args[2]);

		Configuration conf1 = new Configuration();
		Configuration conf2 = new Configuration();

		deletePreviousOutput(conf1, out1);
		deletePreviousOutput(conf1, out2);

		//conf1.set  ("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		//conf2.set  ("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		Job job1 = Job.getInstance(conf1);
		Job job2 = Job.getInstance(conf2);



		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(LongWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);

		job1.setMapperClass(AverageScoreBySubjectAndSchool.MapClass.class);
		job1.setReducerClass(AverageScoreBySubjectAndSchool.Reduce.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		/*job1.setInputFormatClass(KeyValueTextInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);*/

		FileInputFormat.setInputPaths(job1, in);
		FileOutputFormat.setOutputPath(job1, out1);

		job1.setJarByClass(AverageScoreBySubjectAndSchool.class);


		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setMapperClass(HighetScoreAverageScoreBySubject.MapClass.class);
		job2.setReducerClass(HighetScoreAverageScoreBySubject.Reduce.class);

		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

	/*	job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);*/

		FileInputFormat.setInputPaths(job2, out1);
		FileOutputFormat.setOutputPath(job2, out2);

		job2.setJarByClass(HighetScoreAverageScoreBySubject.class);

		ControlledJob cj1 = new ControlledJob(conf1);
		ControlledJob cj2 = new ControlledJob(conf2);

		cj1.setJob(job1);
		cj2.setJob(job2);

		cj2.addDependingJob(cj1);

		JobControl jc = new JobControl("ExamScoresCombined");
		jc.addJob(cj1);
		jc.addJob(cj2);

		Thread t = new Thread(jc);
		t.setDaemon(true);
		t.start();

		while(!jc.allFinished()) {
			Thread.sleep(500);
		}


	}

}