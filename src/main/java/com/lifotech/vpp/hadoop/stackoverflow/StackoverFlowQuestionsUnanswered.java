package com.lifotech.vpp.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class StackoverFlowQuestionsUnanswered extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(StackoverFlowQuestionsUnanswered.class);


    public static class MapClass extends Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            String str = value.toString();

            String[] strings = str.split(",");

            // question or answer and it's post it
            if (strings[0].trim().equals("1")) {
                //1 and questions' post id
                context.write(key, new Text("1"));
            } else {
                //2 and postid

                String answerPostID = strings[1].trim();


                if (answerPostID.length() > 0) {
                    context.write(new Text(answerPostID), new Text("2"));
                }

            }

        }

    }

    private static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

            int response = 0;

            Iterator<Text> it = value.iterator();

            while (it.hasNext()) {
                if (it.next().toString().equals("2")) {
                    response = 1;
                    break;
                }
            }
            if (response == 0) {
                context.write(key, null);
            }


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
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //set mapper and reducer for the job
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);


        // job set jar
        job.setJarByClass(StackoverFlowQuestionsUnanswered.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // job wait for completion for standalone
        return job.waitForCompletion(true) ? 0 : 1;
        // and submit to run in the cluset and restun status code 0
        //job.submit();

        //return 0;
    }

    public static void main(String[] args) {


        try {
            int jobStatus = ToolRunner.run(new Configuration(), new StackoverFlowQuestionsUnanswered(), args);
            System.exit(jobStatus);
        } catch (Exception e) {
            logger.error("The job failed : " + e.getMessage());
        }
    }
}
