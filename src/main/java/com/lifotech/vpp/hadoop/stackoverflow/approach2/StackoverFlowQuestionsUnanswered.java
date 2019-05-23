package com.lifotech.vpp.hadoop.stackoverflow.approach2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import java.util.Iterator;

public class StackoverFlowQuestionsUnanswered extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(StackoverFlowQuestionsUnanswered.class);


    public static class MapClass extends Mapper<Text, Text, Text, StackOverFlowWritable> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {


            StackOverFlowWritable stackOverFlowWritable = new StackOverFlowWritable(key.toString(), value.toString());

            // question or answer and it's post it
            if (stackOverFlowWritable.getPostType().equals("1")) {
                //1 and questions' post id
                context.write(key, stackOverFlowWritable);
            } else {
                //2 and postid
                if (stackOverFlowWritable.getPostType().equals("2")) {
                    context.write(new Text(stackOverFlowWritable.getParentID()), stackOverFlowWritable);
                }

            }

        }

    }

    private static class Reduce extends Reducer<Text, StackOverFlowWritable, Text, NullWritable> {

        public void reduce(Text key, Iterable<StackOverFlowWritable> value, Context context) throws IOException, InterruptedException {

            int response = 0;

            Iterator<StackOverFlowWritable> it = value.iterator();

            while (it.hasNext()) {
                if (it.next().getPostType().equals("2")) {
                    response = 1;
                    break;
                }
            }
            if (response == 0) {
                context.write(key, null);
            }


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
        job.setMapOutputValueClass(StackOverFlowWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //set mapper and reducer for the job
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);


        // job set jar
        job.setJarByClass(StackoverFlowQuestionsUnanswered.class);

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
            int jobStatus = ToolRunner.run(new Configuration(), new StackoverFlowQuestionsUnanswered(), args);
            System.exit(jobStatus);
        } catch (Exception e) {
            logger.error("The job failed : " + e.getMessage());
        }
    }
}
