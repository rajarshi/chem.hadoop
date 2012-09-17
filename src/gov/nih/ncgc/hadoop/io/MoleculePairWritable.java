package gov.nih.ncgc.hadoop.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A writable suitable for storing pairs of SMILES and passing them round mappers and reducers.
 *
 * @author Rajarshi Guha
 */
public class MoleculePairWritable implements WritableComparable<MoleculePairWritable> {
    Text s1, s2, combined;

    public MoleculePairWritable(String s1, String s2) {
        this.s1 = new Text(s1);
        this.s2 = new Text(s2);

        // make sure we combine the SMILES in a fixed order
        if (s1.compareTo(s2) > 0)
            combined = new Text(s1 + s2);
        else if (s1.compareTo(s2) < 0) combined = new Text(s2 + s1);
        else combined = new Text(s1+s2);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.write(s1.getBytes());
        dataOutput.write(s2.getBytes());
    }

    public void readFields(DataInput dataInput) throws IOException {
        s1.readFields(dataInput);
        s2.readFields(dataInput);
    }

    public int compareTo(MoleculePairWritable moleculePairWritable) {
        Text newc = moleculePairWritable.combined;
        return combined.compareTo(newc);
    }
}
