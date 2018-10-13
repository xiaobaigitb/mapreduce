package com.day05;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WorkReduce extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text filename, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuffer buffer = new StringBuffer();
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()){
            Text next = iterator.next();
            buffer.append(next).append(" ");
        }
        context.write(filename, new Text(context.toString()));
    }
}
