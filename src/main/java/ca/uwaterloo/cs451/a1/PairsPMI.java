/* implement for PairsPMI */

package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfWritables;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);

    // Mapper: emits (token, 1) for every distinct word occurrence.
    public static final class SingleWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        // Reuse objects to save overhead of object creation.
        private static final IntWritable ONE = new IntWritable(1);
        private static final Text WORD = new Text();
        private static int WordsCount = 0;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> words = Tokenizer.tokenize(value.toString());
            Iterator<String> wordsIterator = words.iterator();
            HashMap<String, Integer> WordsSet = new HashMap<String, Integer>();
            /* Adds the specified element to the map if it is not already presen */
             while (wordsIterator.hasNext() && WordsCount <= 40) {
                 String word = wordsIterator.next();
                 if (! WordsSet.containsKey(word)) {
                    WordsSet.put(word, 1);
                    WORD.set(word);
                    context.write(WORD, ONE);
                }
                ++WordsCount;
            }
            WordsCount = 0;
            /* use * to represent one line */
            WORD.set("**totalline**");
            context.write(WORD, ONE);
            WordsSet.clear();
        }
    }

    // Reducer: sums up all the counts.
    public static final class SingleWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Reuse objects.
        private static final IntWritable SUM = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Sum up values.
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    public static final class WordPairMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
        // Reuse objects to save overhead of object creation.
        private static final IntWritable ONE = new IntWritable(1);
        private static final PairOfStrings PAIR = new PairOfStrings();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            HashMap<PairOfStrings, Integer> PAIRMAP = new HashMap<PairOfStrings, Integer>();
            for (int i = 0; i < Math.min(40, tokens.size()); i++) {
                for (int j = 0; j < Math.min(40, tokens.size()); j++) {
                    if (i == j) continue;
                    if (tokens.get(i).equals(tokens.get(j))) continue;
                    PAIR.set(tokens.get(i), tokens.get(j));
                    if (!PAIRMAP.containsKey(PAIR)) {
                        context.write(PAIR, ONE);
                        PAIRMAP.put(PAIR, 1);
                    }
                }
            }
            PAIRMAP.clear();
        }
    }

    public static final class WordPairCombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
        // Reuse objects.
        private static final IntWritable SUM = new IntWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Sum up values.
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0; /* count for the pairs */
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    public static final class WordPairReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfWritables> {
        // Reuse objects.
        private static final PairOfWritables<FloatWritable, IntWritable> PMICOUNT = new PairOfWritables<FloatWritable, IntWritable>();
        private static final HashMap<String, Integer> WordsSet = new HashMap<String, Integer>();
        private static long totallines = 0;
        private int threshold = 1;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            threshold = conf.getInt("threshold",1);
            FileSystem fs = FileSystem.get(conf);
            /* path for the output of first job */
            Path sourcefile = new Path("firstjoboutput/");
            /* get list of output files part-r-0000* from firstjoboutput */
            FileStatus[] status = fs.listStatus(sourcefile);
            try {
                for (int i = 0; i < status.length; ++i) {
                    FSDataInputStream in = fs.open(status[i].getPath());
                    InputStreamReader inStream = new InputStreamReader(in);
                    BufferedReader br = new BufferedReader(inStream);
                    String line = br.readLine();
                    while (line != null) {
                        String[] pair = line.split("\\s+");
                        if (pair.length == 2) {
                            WordsSet.put(pair[0], Integer.parseInt(pair[1]));
                        }
                        line = br.readLine();
                    }
                }
                totallines = (long) WordsSet.get("**totalline**");
            } catch (Exception e) {
                throw new IOException("Fail to load data");
            }
        }

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Sum up values.
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0; /* count for the pairs */
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            //System.out.printf("hhahaha"+sum);
            if (sum >= threshold) {
                String lele = key.getLeftElement();
                String rele = key.getRightElement();
                //System.out.println(lele+" "+rele);
                int countX = WordsSet.get(lele);
                //System.out.println(" "+lele+"nihaoya");
                int countY = WordsSet.get(rele);
                //System.out.printf("hahahhelements " + lele + rele);
                float probXY = (float) sum / (float) totallines;
                float probX = (float) countX / (float) totallines;
                float probY = (float) countY / (float) totallines;
                float pmi = (float) Math.log10(probXY / (probX * probY));
                FloatWritable PMI = new FloatWritable();
                IntWritable PC = new IntWritable();
                PMI.set(pmi);
                PC.set(sum);
                PMICOUNT.set(PMI, PC);
                context.write(key, PMICOUNT);
            }
        }
    }

    /**
     * Creates an instance of this tool.
     */
    private PairsPMI() {}

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;

        @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of co-occurrence")
        int numThreshold = 1;
    }

    /**
     * Runs this tool.
     */
    @Override
    public int run(String[] argv) throws Exception {
        final Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        LOG.info("Tool: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - number of reducers: " + args.numReducers);
        LOG.info(" - threshold of co-occurrence" + args.numThreshold);

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName(PairsPMI.class.getSimpleName());
        job.setJarByClass(PairsPMI.class);

        job.setNumReduceTasks(args.numReducers);

        /*job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");*/

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path("firstjoboutput/"));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(SingleWordMapper.class);
        job.setCombinerClass(SingleWordReducer.class);
        job.setReducerClass(SingleWordReducer.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path("firstjoboutput/");
        FileSystem.get(conf).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        // start job2 for the pairs
        Job job2 = Job.getInstance(conf);
        job2.setJobName(PairsPMI.class.getSimpleName()+"PMI");
        job2.setJarByClass(PairsPMI.class);

        job2.setNumReduceTasks(args.numReducers);

        /*job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");*/

        job2.getConfiguration().setInt("threshold", args.numThreshold);

        FileInputFormat.setInputPaths(job2, new Path(args.input));
        FileOutputFormat.setOutputPath(job2, new Path(args.output));

        job2.setMapOutputKeyClass(PairOfStrings.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(PairOfStrings.class);
        job2.setOutputValueClass(PairOfWritables.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setMapperClass(WordPairMapper.class);
        job2.setCombinerClass(WordPairCombiner.class);
        job2.setReducerClass(WordPairReducer.class);

        // Delete the output directory if it exists already.
        Path outputDir2 = new Path(args.output);
        FileSystem.get(conf).delete(outputDir2, true);

        startTime = System.currentTimeMillis();
        job2.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PairsPMI(), args);
    }

}







