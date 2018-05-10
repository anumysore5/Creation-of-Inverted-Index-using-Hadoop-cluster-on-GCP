import java.util.HashMap;
import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: InvertedIndex <input path> <output path>");
			System.exit(-1);
		}

		// creating a hadoop job and assigning a job name for identification
		Job job = new Job();
		job.setJarByClass(InvertedIndex.class);
		job.setJobName("InvertedIndex");

		// The HDFS input and output directories to be fetched from the Dataproc job
		// submission console
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Providing the mapper and reducer class names.
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);

		// Setting the job object with the data types of the output key(text) and
		// value(IntWritable)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(String.class);
		job.waitForCompletion(true);

	}
}

class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static Text docId = null;
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		docId = new Text(value.toString().split("\t")[0]);
		String docValue = value.toString().split("\t")[1];
		StringTokenizer tokenizer = new StringTokenizer(docValue);

		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			context.write(word, docId);
		}
	}
}

class InvertedIndexReducer extends Reducer<Text, Text, Text, String> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int startCount = 1;
		int totalCount = 0;
		HashMap<String, Integer> docMap = new HashMap<>();

		for (Text value : values) {

			// If for a given word, there is already this docID present in the map, take its
			// count and increment by 1 when the same docId is encountered again.
			if (docMap.containsKey(value.toString())) {
				int existingCount = docMap.get(value.toString());
				totalCount = existingCount + 1;
				docMap.put(value.toString(), totalCount);
			}
			// else, insert this docId for the word as the key, and value will be 1.
			else {
				totalCount = startCount;
				docMap.put(value.toString(), totalCount);
			}
		}

		StringBuilder res = new StringBuilder();
		for (String output : docMap.keySet()) {
			res.append("\t" + output + ":" + docMap.get(output));
		}

		context.write(key, res.toString());
	}
}
