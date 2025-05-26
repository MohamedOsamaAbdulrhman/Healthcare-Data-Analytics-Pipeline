import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageAge {

    public static class AgeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text outputKey = new Text("avg_age");
        private IntWritable outputValue = new IntWritable();
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private static final String SENTINEL_DATE = "9999-12-31 00:00:00";

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0 && value.toString().contains("row_id")) {
                return;  // skip header
            }

            String[] fields = value.toString().split(",");
            if (fields.length < 8) return;

            try {
                String dobStr = fields[3];
                String dodStr = fields[4];

                if (dobStr.equals(SENTINEL_DATE)) {
                    return; // no dob, skip
                }

                Date dob = sdf.parse(dobStr);
                Date refDate;

                if (!dodStr.equals(SENTINEL_DATE)) {
                    refDate = sdf.parse(dodStr);
                } else {
                    refDate = new Date(); // current date if no dod
                }

                long diffInMillis = refDate.getTime() - dob.getTime();
                int age = (int) TimeUnit.MILLISECONDS.toDays(diffInMillis) / 365;

                if (age >= 0 && age < 130) {
                    outputValue.set(age);
                    context.write(outputKey, outputValue);
                }
            } catch (Exception e) {
                // ignore bad lines
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            if (count > 0) {
                int average = sum / count;
                context.write(new Text("Average_Age"), new IntWritable(average));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AverageAge <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Patient Age");

        job.setJarByClass(AverageAge.class);
        job.setMapperClass(AgeMapper.class);
        job.setReducerClass(AverageReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
