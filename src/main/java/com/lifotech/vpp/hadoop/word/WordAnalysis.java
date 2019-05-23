package com.lifotech.vpp.hadoop.word;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class WordAnalysis {


    public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable>{
        @Override
        public  void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

            String line = value.toString();

            //System.out.println("MAP CALLED VALUE IS " + line);
            String body = line.toString().replaceAll("[^A-Za-z\\s]", "").toLowerCase();

            String[] words = body.split(" ");

            for(String word: words) {
                if(word.length() >= 7) {
                    context.write(new Text(word), new LongWritable(1));
                }
            }


        }
    }

    public static  class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            Iterator<LongWritable> it = values.iterator();
            Long sum =0l;
            while(it.hasNext()) {
                sum += it.next().get();
            }

            context.write(key,new LongWritable(sum));
        }
    }
/*

    public static void deletePreviousOutput(Configuration configuration, Path path) {

        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            fileSystem.delete(path, true);
        } catch (IOException e) {
            System.out.println(e.getMessage());
            //ignore it
        }
    }
*/

    public static void main(String[] args) {
        try {

            Path in = new Path(args[0]);
            Path out = new Path(args[1]);

            Configuration conf = new Configuration();

            //deletePreviousOutput(conf, out);

            Job job = Job.getInstance(conf);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            job.setMapperClass(MapClass.class);
            job.setReducerClass(Reduce.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job, in);
            FileOutputFormat.setOutputPath(job, out);

            job.setJarByClass(WordAnalysis.class);
            job.submit();
        }catch (IOException e) {
            System.out.println(e.getMessage());

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}


