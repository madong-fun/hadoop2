package com.hadoop2.mapreduce.mapper;

import com.hadoop2.utils.NcdcRecordParser;
import com.hadoop2.utils.NcdcStationMetadataParser;
import com.hadoop2.writable.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by madong on 16-6-5.
 */
public class JoinStationMapper extends Mapper<LongWritable,Text,TextPair,Text> {

    private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (parser.parse(value)){

            context.write(new TextPair(parser.getStationId(),"0"), new Text(parser.getStationName()));
        }

    }
}
