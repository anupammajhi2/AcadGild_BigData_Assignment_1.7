package mapreduce.demo.task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task2_mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|");
		
		Text companyname = new Text(); 
		IntWritable count = new IntWritable();
		
		companyname.set(lineArray[0]);
		count.set(1);
		
		if(!(companyname.toString().equals("NA"))) {
			context.write(companyname,count);
		}
	}
}
