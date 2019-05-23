package com.lifotech.vpp.hadoop.examScore.methodchaining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class HighetScoreAverageScoreBySubject extends Configured implements Tool {

    public static class MapClass extends Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            //Art,Bigtown Academy	62.0

            String line = key.toString();

            String[] tokens = line.split(",");

            //subject,school


            context.write(new Text(tokens[0]), new Text(tokens[1]+","+ value.toString()));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{

            Iterator<Text> it = value.iterator();

            String school = null;
            double avgScore = 0.0d;
            while(it.hasNext()) {
                Text val = it.next();
                String strVal = val.toString();
                //split on comma
                String[] tokens = strVal.split(",");
                if (Double.valueOf(tokens[1]) > avgScore) {
                    avgScore = Double.valueOf(tokens[1]);
                    school = tokens[0];
                }

            }

            context.write(key, new Text(school + "," + avgScore));
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

    @Override
    public int run(String[] strings) throws Exception {
        int result = -1;
        try {

            Configuration configuration = this.getConf();


            //use mapreduce.input.keyvaluelinerecordreader.key.value.separator


            Job job = Job.getInstance(configuration);
            configuration.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");


            job.setMapperClass(HighetScoreAverageScoreBySubject.MapClass.class);
            //job.setCombinerClass(Combiner.class);
            job.setReducerClass(HighetScoreAverageScoreBySubject.Reduce.class);


            //set fileformat
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            Path inputPath = new Path(strings[0]);
            Path outputPath = new Path(strings[1]);

            deletePreviousOutput(configuration, outputPath);

            FileSystem fileSystem = FileSystem.get(configuration);

            fileSystem.delete(outputPath, true);

            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            job.setJarByClass(HighetScoreAverageScoreBySubject.class);
            //result = job.waitForCompletion(true) ? 0 : 1;

            job.submit();
            //return 0;


        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return 0;
    }


    public static void main(String[] args) {

        try {
            int res = ToolRunner.run(new Configuration(), new HighetScoreAverageScoreBySubject(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
