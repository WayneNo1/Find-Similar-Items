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

  public static int FILE_COUNT = 50;  // Number of text files
  public static int HASH_LENGTH = 20000000;  // Number of all shingles
  public static int HASH_FUNC = 100;  // Number of hash functions for Minhashing

  /* Mapper for Shingling on all the text files */
  /* Input: Text files */
  /* Output: <file name> <3-shingles> */
  public static class ShinglingMapper extends Mapper<Object, Text, Text, Text>{
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

    // The range of the hash function: 0 ~ HASH_LENGTH - 1
    private int HASH_LENGTH = 20000000;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String shingleList = new String("");
      boolean first = true;

      for (Text val : values) {
        String shingle = val.toString();

        // Hash the 3-shingle with hashCode() and modulus operation
        int hashShingle = (shingle.hashCode() % HASH_LENGTH + HASH_LENGTH) % HASH_LENGTH;
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

  /* Mapper for Shingling on the shingles to get the signatures */
  /* Input: <file name> <list of hashed 3-shingles> */
  /* Output: <file name> <list of signatures> */
  public static class MinhashingMapper extends Mapper<Object, Text, Text, Text>{
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Scan the input file
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String fileName = itr.nextToken();
        keyText.set(fileName);

        // Get all the hashed shingles
        String shingleList = itr.nextToken();
        String[] shinglesStr = shingleList.split(",");
        int shingles[] = new int[shinglesStr.length];
        for (int i = 0; i < shinglesStr.length; i++) {
          shingles[i] = Integer.parseInt(shinglesStr[i]);
        }

        // Apply Minhashing with 100 hash function, and get the signatures
        boolean first = true;
        String signList = new String("");
        for (int i = 1; i <= HASH_FUNC; i++) {
          int minHash = 2000000099;
          for (int shingle : shingles) {
            // Hash function: i * shingle + (i * 2 - 1) mod HASH_LENGTH
            int hash = (i * shingle + (i * 2 - 1) % HASH_LENGTH + HASH_LENGTH) % HASH_LENGTH;
            minHash = (hash < minHash) ? hash : minHash;
          }
          if (!first) {
            signList += ",";
          }
          signList += String.valueOf(minHash);
          first = false;
        }

        valueText.set(signList);
        context.write(keyText, valueText);
      }
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
    job1.setMapperClass(ShinglingMapper.class);
    job1.setReducerClass(ShinglingReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    Path inPath = new Path(otherArgs[0]);
    Path outPath = new Path(otherArgs[1] + "_1");
    FileInputFormat.setInputPaths(job1, inPath);
    FileOutputFormat.setOutputPath(job1, outPath);
    job1.waitForCompletion(true);

    /* Minhashing on the shingles to get the signatures */
    /* Input: <output file>_1 */
    /* Output: <output file>_2 */
    Job job2 = new Job(conf, "Minhashing");
    job2.setJarByClass(LSH.class);
    job2.setMapperClass(MinhashingMapper.class);
    job2.setNumReduceTasks(0);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    Path inPath = new Path(otherArgs[1] + "_1");
    Path outPath = new Path(otherArgs[1] + "_2");
    FileInputFormat.setInputPaths(job2, inPath);
    FileOutputFormat.setOutputPath(job2, outPath);
    job2.waitForCompletion(true);
  }
}
