package com.hadoop2;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.awt.*;
import java.io.IOException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {

       GraphicsEnvironment environment = GraphicsEnvironment.getLocalGraphicsEnvironment();

       String[] names = environment.getAvailableFontFamilyNames();

        for (int i = 0; i <names.length; i++) {
            System.out.println(names[i]);
        }

        Schema.Parser parser = new Schema.Parser();

        Schema schema = parser.parse(App.class.getResourceAsStream("StringPair.avsc"));

        GenericRecord record = new GenericData.Record(schema);

        record.put("left",new Utf8("L"));
        record.put("right",new Utf8("R"));


    }
}
