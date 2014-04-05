import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class MeanCalculator {

    private static class MeanMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        public void map(LongWritable longWritable, Text text,
                        OutputCollector<IntWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            String[] split = line.split(" ");

            for (int i = 0; i < split.length; i++) {
                int propVal = Integer.parseInt(split[i]);

                outputCollector.collect(new IntWritable(i), new IntWritable(propVal));
            }
        }
    }

    private static class MeanReducer extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, Text> {
        @Override
        public void reduce(IntWritable property, Iterator<IntWritable> propvalues,
                           OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {

            int sum = 0;
            double mean = 0;
            int count = 0;
            while (propvalues.hasNext()) {
                sum += propvalues.next().get();
                count++;
            }

            if (count != 0)
                mean = sum / count;

            while (propvalues.hasNext()) {
                sum += Math.pow(propvalues.next().get() - mean, 2);
            }

            double std = Math.sqrt(sum / count);

            outputCollector.collect(property, new Text(mean + "#" + std));
        }
    }

    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(MeanCalculator.class);
        conf.setJobName("mean");
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(MeanMapper.class);
        // conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(MeanReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
