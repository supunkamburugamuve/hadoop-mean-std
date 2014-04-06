import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class StatisticCalculator {
    /***
     * Read the input as text line by line. Split the line using space (' ') and output key as column number
     * and value the value for that column in the line.
     */
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

    /**
     * Calculate std using one pass incremental algorithm.
     * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
     *
     * The input is
     * key - column number
     * value - list of column values
     */
    private static class MeanReducer extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, Text> {
        @Override
        public void reduce(IntWritable property, Iterator<IntWritable> propvalues,
                           OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {

            int n = 0;
            double mean = 0;
            double M2 = 0;

            while(propvalues.hasNext()) {
                n++;
                int x = propvalues.next().get();
                double delta = x - mean;
                mean = mean + delta / n;

                M2 = M2 + delta * (x - mean);
            }

            double variance = M2 / (n - 1);

            outputCollector.collect(property, new Text(mean + "#" + Math.sqrt(variance)));
        }
    }

    /**
     * Read the input as lines of text. Split the line using ' ' (space) and produce the data for the reducer
     */
    private static class CorrMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable longWritable, Text text,
                        OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            String[] split = line.split(" ");

            for (int i = 0; i < split.length; i++) {
                for (int j = i + 1; j < split.length; j++) {
                    int propValX = Integer.parseInt(split[i]);
                    int propValY =Integer.parseInt(split[j]);

                    outputCollector.collect(new Text(i + "#" + j), new Text(propValX + "#" + propValY));
                }
            }
        }
    }

    /**
     * key columnI#columnJ
     * value list of column I and J values, i.e <1#2, 4#4, 3#9 ...
     */
    private static class CorrReducer extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterator<Text> propvalues,
                           OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {

            double x = 0.0d;
            double y = 0.0d;
            double xx = 0.0d;
            double yy = 0.0d;
            double xy = 0.0d;
            double n = 0.0d;

            while(propvalues.hasNext()) {
                String []split = propvalues.next().toString().split("#");
                if (split.length != 2) {
                    continue;
                }

                double i = Double.parseDouble(split[0]);
                double j = Double.parseDouble(split[1]);

                x += Double.parseDouble(split[0]);
                y += Double.parseDouble(split[1]);

                xx += Math.pow(i, 2.0d);
                yy += Math.pow(j, 2.0d);
                xy += (i * j);
                n += 1.0d;
            }

            double r = 0;
            if (0.0d == n) {
                outputCollector.collect(key, new DoubleWritable(r));
            }

            double numerator = x / n;
            numerator = numerator * y;
            numerator = xy - numerator;

            double denom1 = Math.pow(x, 2.0d) / n;
            denom1 = xx - denom1;

            double denom2 = Math.pow(y, 2.0d) / n;
            denom2 = yy - denom2;

            double denom = denom1 * denom2;
            denom = Math.sqrt(denom);

            r = numerator / denom;

            outputCollector.collect(key, new DoubleWritable(r));
        }
    }

    public static void main(String[] args) throws IOException {
        // calculate mean and standard deviation
        JobConf conf = new JobConf(StatisticCalculator.class);
        conf.setJobName("mean");
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(MeanMapper.class);
        conf.setReducerClass(MeanReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);

        // calculate correlation
        JobConf conf2 = new JobConf(StatisticCalculator.class);
        conf2.setJobName("corr");
        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(Text.class);
        conf2.setMapperClass(CorrMapper.class);
        conf2.setReducerClass(CorrReducer.class);

        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf2, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
        JobClient.runJob(conf2);
    }
}
