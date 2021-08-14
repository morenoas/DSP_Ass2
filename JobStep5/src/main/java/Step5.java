import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

public class Step5 {
    private static final String JOB_NAME = "step 5";
    public static final String DELIMITER = ",";
    private static final String FORTH_STEP_OUTPUT = "s3://dsps212--files/output4";
    private static final String FIFTH_STEP_OUTPUT = "s3://dsps212--files/output5/";
    private static boolean IS_PRINTING;
    private static boolean IS_AGGREGATE;
    private static final String PRINT = "print";
    private static final String MIN_NPMI = "minPMI";
    private static final String REL_MIN_NPMI = "relMinPMI";
    private static double minNPMI;
    private static double minRelNPMI;
    private static final int DECADE_INDEX = 0;
    private static final int NPMI_INDEX = 1;

    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        private static final int NGRAM_INDEX = 0;
        private static final int SUM_NPMI_INDEX = 1;
        private static final int NPMI_INDEX = 2;

        protected void setup(Context context) {
            IS_PRINTING = context.getConfiguration().getBoolean(PRINT, false);
        }

        @Override
        public void map(Text decade, Text value, Context context) throws IOException, InterruptedException {
            if(IS_PRINTING){
                System.err.println("map5 in: " + decade.toString() + " -> " + value);
            }
            Configuration conf = context.getConfiguration();
            String[] fields = value.toString().split(DELIMITER);
            double minPMI = conf.getDouble(MIN_NPMI, -1);
            double relMinPMI = conf.getDouble(REL_MIN_NPMI, -1);
            if (minPMI == -1 | relMinPMI == -1) {
                throw new IllegalArgumentException("minNPMI: " + minNPMI + " minRelNPMI: " + minRelNPMI);
            }
            String NGram = fields[NGRAM_INDEX];
            double sumNpmi = Double.parseDouble(fields[SUM_NPMI_INDEX]);
            double npmi = Double.parseDouble(fields[NPMI_INDEX]);
            if(IS_PRINTING){
                System.err.printf("npmi=%f minPMI=%f sumNPMI=%f relMinPMI=%f \n", npmi, minPMI, sumNpmi, relMinPMI);
            }
            if (npmi > minPMI || (npmi / sumNpmi) > relMinPMI) {
                if(IS_PRINTING){
                    System.err.println("map5 out: " + decade + DELIMITER + npmi + " -> " + NGram);
                }
                context.write(new Text(decade  + DELIMITER + npmi), new Text(NGram));
            }
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> chunks = new ArrayList<>();
            for (Text value : values) {
                chunks.add(value.toString());
            }
            context.write(key, new Text(String.join(DELIMITER,chunks)));
        }
    }

    public static class DoubleComparator extends WritableComparator {
        public DoubleComparator() {
            super(Text.class);
        }
        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            int n1 = WritableUtils.decodeVIntSize(b1[s1]);
            int n2 = WritableUtils.decodeVIntSize(b2[s2]);
            return compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2) *(-1);
        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {

        private static final int DECADE_INDEX = 0;

        @Override
        public int getPartition(Text key, Text value, int numReducers) {
            int decade = Integer.parseInt(key.toString().split(DELIMITER)[DECADE_INDEX]);
            return  decade % numReducers;
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        protected void setup(Context context) {
            IS_PRINTING = context.getConfiguration().getBoolean(PRINT, false);
        }

        @Override
        public void reduce(Text decadeNpmi, Iterable<Text> NGramsChunks, Context context) throws IOException, InterruptedException {
            String [] keyFields = decadeNpmi.toString().split(DELIMITER);
            String decade = keyFields[DECADE_INDEX];
            String npmi = keyFields[NPMI_INDEX];
            for (Text chunk : NGramsChunks) {
                String[] NGrams = chunk.toString().split(DELIMITER);
                for (String nGram : NGrams) { // ngrams of chunk
                    context.write(new Text(decade), new Text(npmi + DELIMITER + nGram));
                }
            }
        }


    }

    private static void parseArgs(String[] args) {
        if (args.length < 5) {
            return;
        }
        IS_AGGREGATE = Boolean.parseBoolean(args[1]);
        System.err.printf("aggregate: %b\n", IS_AGGREGATE);
        IS_PRINTING = Boolean.parseBoolean(args[2]);
        System.err.printf("print: %b\n", IS_PRINTING);
        minNPMI = Double.parseDouble(args[3]);
        minRelNPMI = Double.parseDouble(args[4]);
        System.err.printf("minNPMI: %f minRelNPMI: %f\n", minNPMI, minRelNPMI);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.err.printf("------- Start %s -------\n\n%n", JOB_NAME);
        System.err.printf("args = %s\n", Arrays.toString(args));
        parseArgs(args);
        Configuration conf = new Configuration();
        conf.setBoolean(PRINT, IS_PRINTING);
        conf.setDouble(MIN_NPMI, minNPMI);
        conf.setDouble(REL_MIN_NPMI, minRelNPMI);
        Job job = Job.getInstance(conf, JOB_NAME);
        job.setJarByClass(Step5.class);

        job.setMapperClass(MapperClass.class);
        if (IS_AGGREGATE) {
            job.setCombinerClass(CombinerClass.class);
        }
        job.setSortComparatorClass(DoubleComparator.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(KeyValueTextInputFormat.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(FORTH_STEP_OUTPUT));
        FileOutputFormat.setOutputPath(job, new Path(FIFTH_STEP_OUTPUT));

        // waiting for job to finish
        boolean jobResult = job.waitForCompletion(true);
        System.exit(jobResult ? 0 : 1);
    }

}
