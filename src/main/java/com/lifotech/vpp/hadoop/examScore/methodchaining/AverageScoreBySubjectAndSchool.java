package com.lifotech.vpp.hadoop.examScore.methodchaining;

import com.lifotech.vpp.hadoop.housePrice.HousePriceAnalysisVer3;
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

public class AverageScoreBySubjectAndSchool extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //1,Bigtown Academy,French,36

            String line = value.toString();

            String[] tokens = line.split(",");

            //subject,school
            String subjectAndSchool = tokens[2] + "," + tokens[1];

            context.write(new Text(subjectAndSchool), new LongWritable(Long.valueOf(tokens[3])));
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {
        public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException{

            Iterator<LongWritable> it = value.iterator();

            long sum = 0l;
            long cnt = 0l;

            while(it.hasNext()) {
                long score = it.next().get();
                sum += score;
                cnt++;
            }

            long avg = sum/cnt;

            context.write(key, new Text(String.valueOf(avg)));
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
            //configuration.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");


            job.setMapperClass(AverageScoreBySubjectAndSchool.MapClass.class);
            //job.setCombinerClass(Combiner.class);
            job.setReducerClass(AverageScoreBySubjectAndSchool.Reduce.class);


            //set fileformat
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            Path inputPath = new Path(strings[0]);
            Path outputPath = new Path(strings[1]);

            deletePreviousOutput(configuration, outputPath);

            FileSystem fileSystem = FileSystem.get(configuration);

            fileSystem.delete(outputPath, true);

            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            job.setJarByClass(AverageScoreBySubjectAndSchool.class);
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
            int res = ToolRunner.run(new Configuration(), new AverageScoreBySubjectAndSchool(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
