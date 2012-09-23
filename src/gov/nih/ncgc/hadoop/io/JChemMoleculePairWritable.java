package gov.nih.ncgc.hadoop.io;

import chemaxon.formats.MolExporter;
import chemaxon.formats.MolImporter;
import chemaxon.struc.Molecule;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A writable suitable for storing pairs of Molecule objects and passing them round as values from mappers and reducers.
 *
 * @author Rajarshi Guha
 */
public class JChemMoleculePairWritable implements Writable {
    Molecule s1, s2;

    public JChemMoleculePairWritable() {
        s1 = new Molecule();
        s2 = new Molecule();
    }

    public JChemMoleculePairWritable(Molecule s1, Molecule s2) {
        super();
        this.s1 = s1.cloneMolecule();
        this.s2 = s2.cloneMolecule();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.write(s1.toBinFormat("smiles"));
        dataOutput.write(s2.toBinFormat("smiles"));
    }

    public void readFields(DataInput dataInput) throws IOException {
//        s1 = MolImporter.importMol(dataInput.readFully();)
//        s1 = new Text(dataInput.readUTF());
//        s2 = new Text(dataInput.readUTF());
    }

    public Molecule getS1() {
        return s1;
    }

    public void setS1(Molecule s1) {
        this.s1 = s1;
    }

    public Molecule getS2() {
        return s2;
    }

    public void setS2(Molecule s2) {
        this.s2 = s2;
    }
}
