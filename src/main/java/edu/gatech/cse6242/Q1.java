package edu.gatech.cse6242;

import java.lang.Math;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 {



  public static class TokenizerMapper
       extends Mapper<Object, Text, IntWritable, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private IntWritable src = new IntWritable();
    private IntWritable weight = new IntWritable();
    //private Integer i = new Integer(0);
    //private IntWritable num = new IntWritable(Integer.parseInt(word));

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      
      while (itr.hasMoreTokens()) {
	String line = itr.nextToken();
	String[] numbers = line.split("\t");
        src.set(new Integer (numbers[0]));
	weight.set(new Integer (numbers[2]));
	context.write(src, weight);
      }

	//System.out.println("*****************************");
/*
	while (itr.hasMoreTokens()) {

	i += 1;
	if ( i == 1 ){
	  word.set(itr.nextToken()); 

	} else if ( i == 3){
	  word.set(itr.nextToken()); 
	}

        context.write(word, one);
	System.out.println(word);
	System.out.println(i);
      }

	for (i : 3){
	  System.out.println("val:");
	  System.out.println(val);
	}

	System.out.println("word:");
	System.out.println(word);
	System.out.println("key:");
	System.out.println(key);
	System.out.println("value:");
	System.out.println(value);
*/
    }
  }

  public static class IntSumReducer
       extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int max = 0;
      for (IntWritable val : values) {
	max = Math.max(max, val.get());
      }
      result.set(max);
      context.write(key, result);
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1");

    /* TODO: Needs to be implemented */

    job.setJarByClass(Q1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);


    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
