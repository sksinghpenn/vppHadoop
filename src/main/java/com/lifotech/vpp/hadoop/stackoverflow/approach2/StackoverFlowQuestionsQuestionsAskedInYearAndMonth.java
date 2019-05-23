package com.lifotech.vpp.hadoop.stackoverflow.approach2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class StackoverFlowQuestionsQuestionsAskedInYearAndMonth extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(StackoverFlowQuestionsQuestionsAskedInYearAndMonth.class);


    public static class MapClass extends Mapper<Text, Text, Text, LongWritable> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            StackOverFlowWritable stackOverFlowWritable = new StackOverFlowWritable(key.toString(), value.toString());

            // question or answer and it's post it
            if (stackOverFlowWritable.getPostType().equals("1")) {
                //1 and questions' post id
                logger.info(stackOverFlowWritable.getDateTimeCreated().substring(0, 7));
                context.write(new Text(stackOverFlowWritable.getDateTimeCreated().substring(0, 7)), new LongWritable(1));
            }
            //  ignore posttype 2 as we are looking for answers


        }

    }


    private static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {

            long response = 0L;

            Iterator<LongWritable> it = value.iterator();

            while (it.hasNext()) {
                response += 1;
            }

            context.write(key, new LongWritable(response));


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
    public int run(String[] args) throws Exception {

        // get paths
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // get the conf
        Configuration configuration = getConf();

        // create the job object
        Job job = Job.getInstance(configuration);

        // set fileinput and fileout format
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // set map input/outut and reduce input/output fommat
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //set mapper and reducer for the job
        job.setMapperClass(MapClass.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);



        // job set jar
        job.setJarByClass(StackoverFlowQuestionsQuestionsAskedInYearAndMonth.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        deletePreviousOutput(configuration, outputPath);

        // job wait for completion for standalone
        //return job.waitForCompletion(true) ? 0 : 1;
        // and submit to run in the cluset and restun status code 0
        job.submit();

        return 0;
    }

    public static void main(String[] args) {


        try {
            int jobStatus = ToolRunner.run(new Configuration(), new StackoverFlowQuestionsQuestionsAskedInYearAndMonth(), args);
            System.exit(jobStatus);
        } catch (Exception e) {
            logger.error("The job failed : " + e.getMessage());
        }
    }
}
