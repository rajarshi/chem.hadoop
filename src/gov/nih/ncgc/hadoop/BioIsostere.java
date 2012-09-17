package gov.nih.ncgc.hadoop;

import chemaxon.formats.MolImporter;
import chemaxon.license.LicenseManager;
import chemaxon.license.LicenseProcessingException;
import chemaxon.struc.Molecule;
import gov.nih.ncgc.hadoop.io.MoleculePairWritable;
import org.apache.hadoop.filecache.DistributedCache;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

/**
 * Frequency of heavy atoms in a collection of SMILES.
 * <p/>
 * The cheminformatics version of the WordCount example. To run, ensure that jchem.jar
 * is either in the Hadoop <code>lib</code> directory, specified via <code>-libjars</code>
 * or else made available via the distributed cache. To run:
 * <code>
 * hadoop jar atomcount.jar path_to_smiles output_dir
 * </code>
 */
public class BioIsostere {

    public void configure(JobConf job) {

        try {
            Path[] licFiles = DistributedCache.getLocalCacheFiles(job);
            BufferedReader reader = new BufferedReader(new FileReader(licFiles[0].toString()));
            StringBuilder license = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) license.append(line);
            reader.close();
            LicenseManager.setLicense(license.toString());
        } catch (IOException e) {
        } catch (LicenseProcessingException e) {
        }
    }

    public static class BioisostereMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, MoleculePairWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text smirks = new Text();

        public void map(LongWritable longWritable, Text value, OutputCollector<Text, MoleculePairWritable> output, Reporter reporter) throws IOException {
            String[] toks = value.toString().split("\t");
            Molecule fragment = MolImporter.importMol(toks[0]);

            for (int i = 1; i < toks.length - 1; i++) {
                Molecule m1 = MolImporter.importMol(toks[i]);
                for (int j = i + 1; j < toks.length; j++) {
                    Molecule m2 = MolImporter.importMol(toks[j]);

                    // compare m1 & m2
                    smirks.set("smirks goes here");
                    MoleculePairWritable out = new MoleculePairWritable(toks[i], toks[j]);
                    output.collect(smirks, out);
                }
            }
        }
    }

    public static class MoleculePairReducer extends MapReduceBase implements Reducer<Text, MoleculePairWritable, Text, IntWritable> {


        public void reduce(Text key, Iterator<MoleculePairWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
                     while (values.hasNext()) sum += values.next().get();
                     output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(BioIsostere.class);
        conf.setJobName("bioIsostere");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(MoleculePairWritable.class);

        conf.setMapperClass(BioisostereMapper.class);
        conf.setCombinerClass(MoleculePairReducer.class);
        conf.setReducerClass(MoleculePairReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

    }
}