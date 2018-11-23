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
  public static int SHINGLE_HASH = 20000000;  // Number of all shingles
  public static int MINHASH_COUNT = 100;  // Number of hash functions for Minhashing
  public static int LSH_BAND = 50;  // Number of bands for LSH
  public static int LSH_ROW = 2;  // Number of rows for LSH
  public static int BAND_BUCKET = 20;  // Number of buckets for a band for LSH

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

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String shingleList = new String("");
      boolean first = true;

      for (Text val : values) {
        String shingle = val.toString();

        // Hash the 3-shingle with hashCode() and modulus operation
        int hashShingle = (shingle.hashCode() % SHINGLE_HASH + SHINGLE_HASH) % SHINGLE_HASH;
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
        for (int i = 1; i <= MINHASH_COUNT; i++) {
          int minHash = 2000000099;
          for (int shingle : shingles) {
            // Hash function: i * shingle + (i * 2 - 1) mod SHINGLE_HASH
            int hash = (i * shingle + (i * 2 - 1) % SHINGLE_HASH + SHINGLE_HASH) % SHINGLE_HASH;
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

  /* Mapper for Locality Sensitive Hashing on all the signatures */
  /* Input: <file name> <list of signatures> */
  /* Output: <bucket> <file name> */
  public static class LSHMapper extends Mapper<Object, Text, Text, Text>{
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Scan the input file
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String fileName = itr.nextToken();

        // Get the signatures
        String signList = itr.nextToken();
        String[] signStr = signList.split(",");
        int signatures[] = new int[signStr.length];
        for (int i = 0; i < signStr.length; i++) {
          signatures[i] = Integer.parseInt(signStr[i]);
        }

        // Apply LSH with b = LSH_BAND and r = LSH_ROW
        for (int i = 0; i < LSH_BAND; i++) {
          int signSum = 0;

          // Hash function: i * BAND_BUCKET + (Sum of the signatures in the band) mod BAND_BUCKET
          for (int j = 0; j < LSH_ROW; j++) {
            signSum += signatures[i * LSH_ROW + j];
          }
          int bucketNum = i * BAND_BUCKET + signSum % BAND_BUCKET;

          keyText.set(String.valueOf(bucketNum));
          valueText.set(fileName);
          context.write(keyText, valueText);
        }
      }
    }
  }

  /* Reducer for Locality Sensitive Hashing on all the signatures */
  /* Input: <bucket> <file name> */
  /* Output: <bucket> <list of file names> */
  public static class LSHReducer extends Reducer<Text, Text, Text, Text> {
    private Text valueText = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String fileList = new String("");
      boolean first = true;

      // Get all the file names that belong to the current bucket
      for (Text val : values) {
        String fileName = val.toString();

        if (!first) {
          fileList += ",";
        }
        fileList += fileName;
        first = false;
      }

      valueText.set(fileList);
      context.write(key, valueText);
    }
  }

  /* Mapper for getting all the candidate pairs */
  /* Input: <bucket> <list of file names> */
  /* Output: <candidate pair> <bucket> */
  public static class CandidateMapper extends Mapper<Object, Text, Text, Text>{
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Scan the input file
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String bucket = itr.nextToken();

        // Get the file names
        String fileList = itr.nextToken();
        String[] fileNames = fileList.split(",");
        int fileCount = fileNames.length;

        // Only search the bucket that has multiple items
        if (fileCount > 1) {
          for (int i = 0; i < fileCount; i++) {
            for (int j = i; j < fileCount; j++) {
              if (fileNames[i] != fileNames[j]) {
                // Get the candidate pairs
                int fileNum1 = Integer.parseInt(fileNames[i].substring(0, 3));
                int fileNum2 = Integer.parseInt(fileNames[j].substring(0, 3));
                if (fileNum1 < fileNum2) {
                  keyText.set("(" + fileNames[i] + ", " + fileNames[j] + ")");
                }
                else {
                  keyText.set("(" + fileNames[j] + ", " + fileNames[i] + ")");
                }
                valueText.set(bucket);
                context.write(keyText, valueText);
              }
            }
          }
        }
      }
    }
  }

  /* Reducer for getting all the candidate pairs */
  /* Input: <candidate pair> <bucket> */
  /* Output: <candidate pair> <list of buckets> */
  public static class CandidateReducer extends Reducer<Text, Text, Text, Text> {
    private Text valueText = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String bucketList = new String("");
      boolean first = true;

      // Get all the buckets that belong to the candidate pair
      for (Text val : values) {
        String bucket = val.toString();

        if (!first) {
          bucketList += ",";
        }
        bucketList += bucket;
        first = false;
      }

      valueText.set(bucketList);
      context.write(key, valueText);
    }
  }

  /* Mapper for calculating Jaccard Similarity for each candidate pair */
  /* Input: <candidate pair> <list of buckets> */
  /* Output: <similarity> <candidate pair> */
  public static class SimilarityMapper extends Mapper<Object, Text, Text, Text>{
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Scan the input file
      StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
      while (itr.hasMoreTokens()) {
        String pair = itr.nextToken();
        if (!itr.hasMoreTokens()) {
          break;
        }

        // Get the list of buckets
        String bucketList = itr.nextToken();
        String[] buckets = bucketList.split(",");
        int bucketCount = buckets.length;

        // Calculate the Jaccard Similarity and inverse it in order to sort
        double similarity = (double)bucketCount / (double)(LSH_BAND + LSH_BAND - bucketCount);
        similarity = 1.0 / similarity;

        // Keep the first six decimals of the similarity
        keyText.set(String.valueOf(similarity));
        valueText.set(pair);
        context.write(keyText, valueText);
      }
    }
  }

  /* Reducer for calculating Jaccard Similarity for each candidate pair */
  /* Input: <similarity> <candidate pair> */
  /* Output: <candidate pair> <similarity> */
  public static class SimilarityReducer extends Reducer<Text, Text, Text, Text> {
    private Text valueText = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // Get the correct similarity back
      String simStr = key.toString();
      double similarity = Double.parseDouble(simStr);
      similarity = 1.0 / similarity;

      // Get all the candidate pairs that have the current similarity
      for (Text val : values) {
        valueText.set(String.format("%.6f", similarity));
        context.write(val, valueText);
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
    FileInputFormat.setInputPaths(job1, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1] + "_1"));
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
    FileInputFormat.setInputPaths(job2, new Path(otherArgs[1] + "_1"));
    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + "_2"));
    job2.waitForCompletion(true);

    /* Locality Sensitive Hashing on the signatures */
    /* Input: <output file>_2 */
    /* Output: <output file>_3 */
    Job job3 = new Job(conf, "LSH");
    job3.setJarByClass(LSH.class);
    job3.setMapperClass(LSHMapper.class);
    job3.setReducerClass(LSHReducer.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job3, new Path(otherArgs[1] + "_2"));
    FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1] + "_3"));
    job3.waitForCompletion(true);

    /* Get all the candidate pairs */
    /* Input: <output file>_3 */
    /* Output: <output file>_4 */
    Job job4 = new Job(conf, "Candidate Pairs");
    job4.setJarByClass(LSH.class);
    job4.setMapperClass(CandidateMapper.class);
    job4.setReducerClass(CandidateReducer.class);
    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job4, new Path(otherArgs[1] + "_3"));
    FileOutputFormat.setOutputPath(job4, new Path(otherArgs[1] + "_4"));
    job4.waitForCompletion(true);

    /* Calculate the similarity for each candidate pair, and sort them */
    /* Input: <output file>_4 */
    /* Output: <output file> */
    Job job5 = new Job(conf, "Similarity");
    job5.setJarByClass(LSH.class);
    job5.setMapperClass(SimilarityMapper.class);
    job5.setReducerClass(SimilarityReducer.class);
    job5.setOutputKeyClass(Text.class);
    job5.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job5, new Path(otherArgs[1] + "_4"));
    FileOutputFormat.setOutputPath(job5, new Path(otherArgs[1]));
    job5.waitForCompletion(true);
  }
}
