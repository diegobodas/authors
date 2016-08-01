package com.app.countpapers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerCountPapers extends
    Reducer<Text, IntWritable, Text, LongWritable> {

  private LongWritable countAllWords = new LongWritable();

  @Override
  protected void reduce(Text WordKey, Iterable<IntWritable> ListOfOnes,
      Context context)
      throws IOException, InterruptedException {

    long count1s = 0;
    for (@SuppressWarnings("unused")
    IntWritable one : ListOfOnes) {
      count1s++;
    }
    countAllWords.set(count1s);
    context.write(WordKey, countAllWords);
  }

}
