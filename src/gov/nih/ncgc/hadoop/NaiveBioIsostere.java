package gov.nih.ncgc.hadoop;

import gov.nih.ncgc.chemical.chemical;
import gov.nih.ncgc.jchemical.Jchemical;
import test.BioIsostereReplace;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An all pairs bioisostere identification routine.
 * <p/>
 * Purely for comparison purposes. Probably should only use
 * this for a few hundred molecules.
 *
 * @author Rajarshi Guha
 */
public class NaiveBioIsostere {

    class Scaffold {
        String smi;
        List<String> members;

        public Scaffold() {
            members = new ArrayList<String>();
        }

        public String getSmi() {
            return smi;
        }

        public void setSmi(String smi) {
            this.smi = smi;
        }

        public List<String> getMembers() {
            return members;
        }

        public void setMembers(List<String> members) {
            this.members = members;
        }

        public void addMember(String member) {
            this.members.add(member);
        }
    }

    public void run(String inFile) throws Exception {
        List<Scaffold> scaffolds = new ArrayList<Scaffold>();
        BufferedReader reader = new BufferedReader(new FileReader(inFile));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] toks = line.split("\t");
            Scaffold s = new Scaffold();
            s.setSmi(toks[0]);
            for (int i = 1; i < toks.length; i++) s.addMember(toks[i]);
            scaffolds.add(s);
        }
        reader.close();
        System.out.println("Loaded " + scaffolds.size() + " mols");

        BioIsostereReplace bio = new BioIsostereReplace();
        for (Scaffold scaffold : scaffolds) {
            System.out.println("Scaffold: " + scaffold.getSmi() + " with " + scaffold.getMembers().size() + " members");
            List<String> mems = scaffold.getMembers();
            Set<String> usmirks = new HashSet<String>();
            int nsmirk = 0;

            chemical scaf = new Jchemical();
            scaf.load(scaffold.getSmi(), chemical.FORMAT_SMILES);
            bio.setScaffold(scaf);

            for (int i = 0; i < mems.size() - 1; i++) {
                chemical mol1 = new Jchemical();
                mol1.load(mems.get(i), chemical.FORMAT_SMILES);
                bio.setQuery(mol1);

                for (int j = i + 1; j < mems.size(); j++) {
                    chemical mol2 = new Jchemical();
                    mol2.load(mems.get(j), chemical.FORMAT_SMILES);
                    bio.setTarget(mol2);

                    List<String> smirks = bio.getSmirksList();
                    nsmirk += smirks.size();
                    usmirks.addAll(smirks);
                }
            }
            System.out.println("\tGot " + usmirks.size() + " unique SMIRKS from " + nsmirk + " total SMIRKS");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: java -jar bisos.jar gov.nih.ncgc.hadoop.NaiveBioIsostere INPUT_FILE");
            System.out.println("\nINPUT_FILE should be of the form: FRAG_SMI MEM_SMI1 MEM_SMI2 ...");
            System.out.println("where MEM_SMI1 etc are SMILES for the compounds containing FRAG_SMI\n");
            System.exit(-1);
        }
        NaiveBioIsostere nbi = new NaiveBioIsostere();
        nbi.run(args[0]);
    }
}
