package com.lifotech.vpp.hadoop.examScore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;
import java.util.Iterator;

public class AverageScoreAnalysis {


    //create MapClass
    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");

            if ((words[2] != null) && (words[3] != null)) {
                context.write(new Text(words[2].trim()), new Text(words[3]));
            }



        }
    }

    //optional create Combiner

    //create ReduceClass
    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Iterator<Text> it = values.iterator();
            int count = 0;
            long sum = 0l;
            while (it.hasNext()) {
                sum += Long.valueOf(it.next().toString());
                count++;
            }
            double avg = sum / count;

            context.write(key, new DoubleWritable(Double.valueOf(avg)));
        }

    }

    public static void deletePreviousOutput(Configuration configuration, Path path) {

        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            fileSystem.delete(path, true);
        } catch (IOException e) {
            System.out.println(e.getMessage());
            //ignore it
        }
    }

    //test locally and test pseudo distributed modde
    public static void main(String[] args) {

        //get the configuration
        Configuration configuration = new Configuration();


        try {
            //create job object
            Job job = Job.getInstance(configuration);



            //set Mapper
            //set Combiner
            //set Reducer
            job.setMapperClass(MapClass.class);
            job.setReducerClass(Reduce.class);

            //set MapOutputKeyClass MapOutputValueClass
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            //set OutputKeyClass OutputValueclass
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);


            //setInputFormat and setOutputForamt
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            //get the input and output path
            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);


            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            deletePreviousOutput(configuration,outputPath );

            job.setJarByClass(AverageScoreAnalysis.class);

            //submit.job

            job.submit();


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }
}
