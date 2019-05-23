package com.lifotech.vpp.hadoop.housePrice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class HousePriceAnalysisVer2 {

    //Mapper Class
    public static class MapClass extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] tokens = line.split(",");

            //context.write(new Text(tokens[0].trim()), new DoubleWritable(Double.valueOf(tokens[1].trim())));
            context.write(new Text(key), new Text(value+",1"));

        }
    }

    // Combiner Class
    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

            double sum = 0.0d;
            int count = 0;

            Iterator<Text> it = value.iterator();

            while (it.hasNext()) {
               String[] split = it.next().toString().split(",");
               sum += Double.valueOf(split[0]);
               count += Integer.valueOf(split[1]);
            }

            context.write(key, new Text(sum + ","+count));


        }
    }

    // Reducer Class
    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

            double sum = 0.0d;
            int count = 0;

            Iterator<Text> it = value.iterator();

            while (it.hasNext()) {
                String[] split = it.next().toString().split(",");
                sum += Double.valueOf(split[0]);
                count += Integer.valueOf(split[1]);
            }


            context.write(key, new DoubleWritable(sum / count));


        }
    }

    public static void main(String[] args) {

        try {


            Configuration configuration = new Configuration();

            configuration.set("key.value.separator.in.input.line",",");

            Job job = Job.getInstance(configuration);


            job.setMapperClass(MapClass.class);
            job.setCombinerClass(Combiner.class);
            job.setReducerClass(Reduce.class);


            //set fileformat
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);

            FileSystem fileSystem = FileSystem.get(configuration);

            fileSystem.delete(outputPath,true);

            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            job.setJarByClass(HousePriceAnalysisVer2.class);
            job.submit();


        } catch (Exception e) {
            System.out.println(e.getMessage());
        }


    }

}
