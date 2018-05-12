package mapreduce.demo.task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class Task2_reducer2 extends Reducer<Text, IntWritable, Text, IntWritable>
{	
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		int TotalSale = 0;
		for (IntWritable value : values) {
			TotalSale = TotalSale + value.get();
		}

		context.write(key, new IntWritable(TotalSale));
	}
}
