package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LSH {

  public static int FILE_COUNT = 50;

  /* Mapper for Shingling on all the text files */
  /* Input: Text files */
  /* Output: <file name> <3-shingles> */
  public static class ShinglingrMapper extends Mapper<Object, Text, Text, Text>{
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Get the file name
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

      // Tranafer the texts to lower case, and split them
      String valueStr = value.toString().toLowerCase();
      StringTokenizer itr = new StringTokenizer(valueStr, " \t\b\n\'\".,-");
      String str1 = new String("");
      String str2 = new String("");
      String str3 = new String("");

      if (itr.hasMoreTokens()) {
        str1 = itr.nextToken();
      }
      if (itr.hasMoreTokens()) {
        str2 = itr.nextToken();
      }
      while (itr.hasMoreTokens()) {
        str3 = itr.nextToken();
        keyText.set(fileName);

        // Get a 3-shingle
        valueText.set(str1 + " " + str2 + " " + str3);
        context.write(keyText, valueText);

        str1 = str2;
        str2 = str3;
      }
    }
  }

  /* Reducer for Shingling on all the text files */
  /* Input: <file name> <3-shingles> */
  /* Output: <file name> <list of hashed 3-shingles> */
  public static class ShinglingReducer extends Reducer<Text, Text, Text, Text> {
    private Text valueText = new Text();

    // The range of the hash function: 0 ~ hashLength - 1
    private int hashLength = 100000000;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String shingleList = new String("");
      boolean first = true;

      for (Text val : values) {
        String shingle = val.toString();

        // Hash the 3-shingle with hashCode() and modulus operation
        int hashShingle = (shingle.hashCode() % hashLength + hashLength) % hashLength;
        if (!first) {
          shingleList += ",";
        }
        shingleList += String.valueOf(hashShingle);
        first = false;
      }

      valueText.set(shingleList);
      context.write(key, valueText);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: LSH <in-directory> <out-file>");
      System.exit(2);
    }

    /* Shingling on all the text files */
    /* Input: The directory of all the text files */
    /* Output: <output file>_1 */
    Job job1 = new Job(conf, "Shingling");
    job1.setJarByClass(LSH.class);
    job1.setMapperClass(ShinglingrMapper.class);
    job1.setReducerClass(ShinglingReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    Path inPath = new Path(otherArgs[0]);
    Path outPath = new Path(otherArgs[1] + "_1");
    FileInputFormat.setInputPaths(job1, inPath);
    FileOutputFormat.setOutputPath(job1, outPath);
    job1.waitForCompletion(true);
  }
}
