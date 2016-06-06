package com.hadoop2.mapreduce.join;

import com.hadoop2.mapreduce.mapper.JoinRecordMapper;
import com.hadoop2.mapreduce.reduce.JoinReducer;
import com.hadoop2.writable.TextPair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * reduce side join
 * Created by madong on 16-6-5.
 */
public class JoinRecordWithStationName extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(),"Join weather records with station names");
        job.setJarByClass(getClass());

        Path ncdcInputPath = new Path(args[0]);
        Path stationInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        MultipleInputs.addInputPath(job,ncdcInputPath, TextInputFormat.class, JoinRecordMapper.class);
        MultipleInputs.addInputPath(job,stationInputPath,TextInputFormat.class,JoinRecordMapper.class);

        FileOutputFormat.setOutputPath(job,outputPath);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setSortComparatorClass(TextPair.FirstComparator.class);

        job.setMapOutputKeyClass(TextPair.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class KeyPartitioner extends Partitioner<TextPair, Text> {

        @Override
        public int getPartition(TextPair textPair, Text text, int i) {
            return (/*[*/textPair.getFirst().hashCode()/*]*/ & Integer.MAX_VALUE) % i;
        }
    }


}
