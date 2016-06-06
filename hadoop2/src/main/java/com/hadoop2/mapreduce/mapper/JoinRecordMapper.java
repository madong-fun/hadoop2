package com.hadoop2.mapreduce.mapper;


import com.hadoop2.utils.NcdcRecordParser;
import com.hadoop2.writable.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * reduce-side join mapper
 * Created by madong on 16-6-5.
 */
public class JoinRecordMapper extends Mapper<LongWritable,Text,TextPair,Text> {

    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        parser.parse(value);
        context.write(new TextPair(parser.getStationId(),"1"),value);
    }
}
