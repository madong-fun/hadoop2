package com.hadoop2.mapreduce.sort;

import com.hadoop2.utils.JobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by madong on 16-6-4.
 */
public class TotalSortMapReduce extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(
                new TotalSortMapReduce(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] strings) throws Exception {

        Job job = JobBuilder.parseInputAndOutput(this, getConf(), strings);

        if (job == null) return 0;

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        //partitioner
        job.setPartitionerClass(TotalOrderPartitioner.class);
        //採樣率 0.1 採用數量 10000  切分數量 10
        InputSampler.RandomSampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);
        InputSampler.writePartitionFile(job, sampler);//設置採樣器

        //add distributeCache
        Configuration configuration = job.getConfiguration();
        String file = TotalOrderPartitioner.getPartitionFile(configuration);
        URI uri = new URI(file);
        job.addCacheFile(uri);
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
