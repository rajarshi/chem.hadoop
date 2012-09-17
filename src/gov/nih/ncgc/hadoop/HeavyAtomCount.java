package gov.nih.ncgc.hadoop;

import chemaxon.formats.MolImporter;
import chemaxon.struc.MolAtom;
import chemaxon.struc.Molecule;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * Frequency of heavy atoms in a collection of SMILES.
 * <p/>
 * The cheminformatics version of the WordCount example. To run, ensure that jchem.jar
 * is either in the Hadoop <code>lib</code> directory, specified via <code>-libjars</code>
 * or else made available via the distributed cache. To run:
 * <code>
 *     hadoop jar atomcount.jar path_to_smiles output_dir
 * </code>
 */
public class HeavyAtomCount {

    public static class TokenizerMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            Molecule mol = MolImporter.importMol(value.toString());
            for (int i = 0; i < mol.getAtomCount(); i++) {
                MolAtom atom = mol.getAtom(i);
                word.set(atom.getSymbol());
                output.collect(word, one);
            }
        }
    }

    public static class IntSumReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key,
                           Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) sum += values.next().get();
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(HeavyAtomCount.class);
        conf.setJobName("heavyAtomCount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(TokenizerMapper.class);
        conf.setCombinerClass(IntSumReducer.class);
        conf.setReducerClass(IntSumReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

    }
}