package com.lifotech.vpp.hadoop.housePrice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class HousePriceAnalysisVer3 extends Configured implements Tool{



    //Mapper Class
    public static class MapClass extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            //String[] tokens = line.split(",");

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

            long sum = 0L;
            int count = 0;

            Iterator<Text> it = value.iterator();

            while (it.hasNext()) {
                String[] split = it.next().toString().split(",");
                sum += Integer.valueOf(split[0].trim());
                count += Integer.valueOf(split[1].trim());
            }

            context.write(key, new DoubleWritable(sum / count));

        }
    }


    @Override
    public int run(String[] args) throws Exception {
        int result = -1;
        try {

            Configuration configuration = this.getConf();


            //use mapreduce.input.keyvaluelinerecordreader.key.value.separator


            Job job = Job.getInstance(configuration);
            configuration.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");


            job.setMapperClass(MapClass.class);
            //job.setCombinerClass(Combiner.class);
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

            job.setJarByClass(HousePriceAnalysisVer3.class);
            result = job.waitForCompletion(true)?0:1;

            //job.submit();
            //return 0;




        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return 0;
    }

    public static void main(String[] args) {

        try {
            int res = ToolRunner.run(new Configuration(), new HousePriceAnalysisVer3(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
