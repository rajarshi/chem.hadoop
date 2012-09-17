package gov.nih.ncgc.hadoop.pig;

import chemaxon.formats.MolImporter;
import chemaxon.sss.search.MolSearch;
import chemaxon.sss.search.SearchException;
import chemaxon.struc.Molecule;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class SMATCH extends FilterFunc {
    static MolSearch search = null;

    public Boolean exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() < 2) return false;
        String target = (String) tuple.get(0);
        String query = (String) tuple.get(1);
        try {
            Molecule queryMol = MolImporter.importMol(query, "smarts");
            search.setQuery(queryMol);
            search.setTarget(MolImporter.importMol(target, "smiles"));
            return search.isMatching();
        } catch (SearchException e) {
            e.printStackTrace();
        }
        return false;
    }
}
