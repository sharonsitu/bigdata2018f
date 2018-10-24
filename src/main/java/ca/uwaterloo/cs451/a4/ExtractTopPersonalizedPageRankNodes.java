/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a4;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.pair.PairOfIntFloat;
import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;
import tl.lin.data.array.ArrayListOfFloatsWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;
import java.util.List;
import java.util.ArrayList;


public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static class MyMapper extends
          Mapper<IntWritable, PageRankNode, IntWritable, PairOfIntFloat> {
    // define list of queues for sources
    private List<TopScoredObjects<Integer>> queues = new ArrayList<>();
    private String[] sources;

    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      sources = context.getConfiguration().getStrings(SOURCES);
      for (int i = 0; i < sources.length; ++i) {
        queues.add(new TopScoredObjects<>(k));
      }
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
            InterruptedException {
      ArrayListOfFloatsWritable pageranks = node.getPageRanks();
      for (int i = 0; i < sources.length; ++i) {
        float rank = (float) Math.exp(pageranks.get(i)); // the original probability is log-probability
        queues.get(i).add(node.getNodeId(),rank);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      for (int i = 0; i < sources.length; ++i) {
        for (PairOfObjectFloat<Integer> pair : queues.get(i).extractAll()) {
          key.set(i);
          PairOfIntFloat p = new PairOfIntFloat(pair.getLeftElement(),pair.getRightElement());
          context.write(key, p);
        }
      }
    }
  }

  private static class MyReducer extends
          Reducer<IntWritable, PairOfIntFloat, IntWritable, FloatWritable> {
    private List<TopScoredObjects<Integer>> queues = new ArrayList<>();
    private static String[] sources;

    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      sources = context.getConfiguration().getStrings(SOURCES);
      for (int i = 0; i < sources.length; ++i) {
        queues.add(new TopScoredObjects<>(k));
      }
    }

    @Override
    public void reduce(IntWritable id, Iterable<PairOfIntFloat> iterable, Context context)
            throws IOException {
      Iterator<PairOfIntFloat> iter = iterable.iterator();
      while (iter.hasNext()) {
        PairOfIntFloat pair = iter.next();
        Integer nid = pair.getLeftElement();
        Float rank = pair.getRightElement();
        queues.get(id.get()).add(nid,rank);
      }
      // Shouldn't happen. Throw an exception.
      if (iter.hasNext()) {
        throw new RuntimeException();
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();
      for (int i = 0; i < sources.length; ++i) {
        for (PairOfObjectFloat<Integer> pair : queues.get(i).extractAll()) {
          key.set(pair.getLeftElement());
          // We're outputting a string so we can control the formatting.
          value.set(pair.getRightElement());
          context.write(key, value);
        }
      }
    }
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";
  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
            .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
            .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
            .withDescription("top n").create(TOP));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
            .withDescription("num of sources").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sourceList = cmdline.getOptionValue(SOURCES);
    String[] sources = sourceList.split(",");

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);
    LOG.info(" - top: " + n);
    LOG.info(" - numSources: " + sourceList);

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);
    conf.setStrings(SOURCES,sources);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PairOfIntFloat.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PairOfIntFloat.class);
    // Text instead of FloatWritable so we can control formatting

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    Path path = new Path(outputPath);
    FileSystem fs = FileSystem.get(conf);
    for (FileStatus file : fs.listStatus(path)) {
      Path output = file.getPath();
      if (!output.toString().contains("_SUCCESS")) {
        FSDataInputStream inputStream = fs.open(output);
        Scanner scanner = new Scanner(inputStream);
        for (int i = 0; i < sources.length; i++) {
          System.out.println("Source: " + sources[i]);
          for (int j = 0; j < n; j++) {
            int nodeid = scanner.nextInt();
            float pagerank = scanner.nextFloat();
            System.out.printf("%.5f %d", pagerank, nodeid);
            System.out.println();
          }
          System.out.println();
        }
        inputStream.close();
        scanner.close();
      }
    }

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
}
