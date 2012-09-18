package gov.nih.ncgc.hadoop.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A writable suitable for storing pairs of SMILES and passing them round as values from mappers and reducers.
 *
 * @author Rajarshi Guha
 */
public class MoleculePairWritable implements Writable {
    Text s1 = null, s2 = null;

    public MoleculePairWritable() {
        s1 = new Text();
        s2 = new Text();
    }

    public MoleculePairWritable(String s1, String s2) {
        super();
        this.s1 = new Text(s1);
        this.s2 = new Text(s2);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(s1.toString());
        dataOutput.writeUTF(s2.toString());
    }

    public void readFields(DataInput dataInput) throws IOException {
        s1 = new Text(dataInput.readUTF());
        s2 = new Text(dataInput.readUTF());
    }

    public Text getS1() {
        return s1;
    }

    public void setS1(Text s1) {
        this.s1 = s1;
    }

    public Text getS2() {
        return s2;
    }

    public void setS2(Text s2) {
        this.s2 = s2;
    }

    public String toString() {
        return s1.toString() + "\t" + s2.toString();
    }
}
