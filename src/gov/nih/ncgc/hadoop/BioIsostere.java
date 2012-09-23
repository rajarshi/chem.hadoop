package gov.nih.ncgc.hadoop;

import gov.nih.ncgc.chemical.chemical;
import gov.nih.ncgc.hadoop.io.MoleculePairWritable;
import gov.nih.ncgc.jchemical.Jchemical;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import test.BioIsostereReplace;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

/**
 * Generate pairwise differences between molecules.
 * <p/>
 * Serves as the basis for a bioisostere analysis procedure.
 * <code>
 * hadoop jar bisos frag_file output_file license_file
 * </code>
 */
public class BioIsostere extends Configured implements Tool {

    public static class BioisostereMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, MoleculePairWritable> {

        static enum Counters {INPUT_FRAGS, N_COMP}

        private long ncomp = 0;
        private Text smirks = new Text();

        public void configure(JobConf job) {

            try {
                Path[] licFiles = DistributedCache.getLocalCacheFiles(job);
                BufferedReader reader = new BufferedReader(new FileReader(licFiles[0].toString()));
                StringBuilder license = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) license.append(line);
                reader.close();
//                LicenseManager.setLicense(license.toString());
            } catch (IOException e) {
            }
        }

        public void map(LongWritable longWritable, Text value, OutputCollector<Text, MoleculePairWritable> output, Reporter reporter) throws IOException {

            try {
                String[] toks = value.toString().split("\t");
                BioIsostereReplace bir = new BioIsostereReplace();

                chemical scaf = new Jchemical();
                scaf.load(toks[0], chemical.FORMAT_SMILES);
                bir.setScaffold(toks[0]);

                if (toks.length - 1 > 500) {
                    System.out.println("Populous scaffold! " + (toks.length - 1) + " members");
                }

                for (int i = 1; i < toks.length - 1; i++) {

                    chemical jchem1 = new Jchemical();
                    jchem1.load(toks[i], chemical.FORMAT_SMILES);
                    bir.setQuery(jchem1);

                    for (int j = i + 1; j < toks.length; j++) {

                        chemical jchem2 = new Jchemical();
                        jchem2.load(toks[j], chemical.FORMAT_SMILES);
                        bir.setTarget(jchem2);


                        for (String aSmirk : bir.getSmirksList()) {
                            smirks.set(aSmirk);
                            output.collect(smirks, new MoleculePairWritable(toks[i], toks[j]));
                        }

                        reporter.incrCounter(Counters.N_COMP, 1);

                        if (++ncomp % 100 == 0) {
                            reporter.setStatus("Performed " + ncomp + " comparisons [on " + (toks.length - 1) + " members]");
                            reporter.progress();
                        }
                    }
                }
                reporter.incrCounter(Counters.INPUT_FRAGS, 1);
            } catch (Exception e) {

            }
        }
    }

    public static class MoleculePairReducer extends MapReduceBase implements Reducer<Text, MoleculePairWritable, WritableComparable<?>, IntWritable> {

        public void reduce(Text key,
                           Iterator<MoleculePairWritable> values,
                           OutputCollector<WritableComparable<?>, IntWritable> reduceOutput, Reporter reporter) throws IOException {

            int sum = 0;
            while (values.hasNext()) {
                values.next(); // throw away the value
                sum++;
            }
            IntWritable value = new IntWritable(sum);
            reduceOutput.collect(key, value);
        }
    }

    public int run(String[] args) throws Exception {
        JobConf jobConf = new JobConf(getConf(), BioIsostere.class);
        jobConf.setJobName(BioIsostere.class.getSimpleName());

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(MoleculePairWritable.class);

        jobConf.setMapperClass(BioisostereMapper.class);
        jobConf.setReducerClass(MoleculePairReducer.class);

//        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setInt("mapred.line.input.format.linespermap", 10);
        jobConf.setInputFormat(NLineInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        if (args.length != 3) {
            System.err.println("Usage: bisos <datafile> <out> <license file>");
            System.exit(2);
        }

        FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        // make the license file available vis dist cache
        DistributedCache.addCacheFile(new Path(args[2]).toUri(), jobConf);

        long start = System.currentTimeMillis();
        JobClient.runJob(jobConf);
        double duration = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("Total runtime was " + duration + "s");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Using JChem: " + chemaxon.jchem.VersionInfo.JCHEM_VERSION);
        int res = ToolRunner.run(new Configuration(), new BioIsostere(), args);
        System.exit(res);

    }
}