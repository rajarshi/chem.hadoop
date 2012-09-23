package gov.nih.ncgc.hadoop;

import chemaxon.formats.MolFormatException;
import chemaxon.formats.MolImporter;
import chemaxon.sss.search.MolSearch;
import chemaxon.sss.search.SearchException;
import chemaxon.struc.Molecule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

/**
 * SMARTS searching over a set of files using Hadoop.
 *
 * @author Rajarshi Guha
 */
public class SmartsSearch extends Configured implements Tool {
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);

    public static class MoleculeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private String pattern = null;
        private MolSearch search;

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

            pattern = job.getStrings("pattern")[0];
            search = new MolSearch();
            try {
                Molecule queryMol = MolImporter.importMol(pattern);
                search.setQuery(queryMol);
            } catch (MolFormatException e) {
            }

        }

        final static IntWritable one = new IntWritable(1);
        Text matches = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            Molecule mol = MolImporter.importMol(value.toString());
            matches.set(mol.getName());
            search.setTarget(mol);
            try {
                if (search.isMatching()) {
                    output.collect(matches, one);
                } else {
                    output.collect(matches, zero);
                }
            } catch (SearchException e) {
            }
        }
    }

    public static class SmartsMatchReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key,
                           Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
            while (values.hasNext()) {
                if (values.next().compareTo(one) == 0) {
                    result.set(1);
                    output.collect(key, result);
                }
            }
        }
    }

    public int run(String[] args) throws Exception {
        JobConf jobConf = new JobConf(getConf(), HeavyAtomCount.class);
        jobConf.setJobName("smartsSearch");

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setMapperClass(MoleculeMapper.class);
        jobConf.setCombinerClass(SmartsMatchReducer.class);
        jobConf.setReducerClass(SmartsMatchReducer.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        jobConf.setNumMapTasks(5);

        if (args.length != 4) {
            System.err.println("Usage: ss <in> <out> <pattern> <license file>");
            System.exit(2);
        }

        FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
        jobConf.setStrings("pattern", args[2]);

        // make the license file available vis dist cache
        DistributedCache.addCacheFile(new Path(args[3]).toUri(), jobConf);

        JobClient.runJob(jobConf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Using JChem: " + chemaxon.jchem.VersionInfo.JCHEM_VERSION);
        int res = ToolRunner.run(new Configuration(), new SmartsSearch(), args);
        System.exit(res);
    }
}
