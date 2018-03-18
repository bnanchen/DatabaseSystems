package ch.epfl.dias.store.PAX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;
import sun.nio.cs.StandardCharsets;

public class PAXStore extends Store {

    // TODO: Add required structures
    private DBPAXpage table[];
    private Path path;
    private DataType[] schema;
    private String delimiter;
    private int tuplesPerPage;
    private List<String> lines;

    public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
        // TODO: Implement
        this.schema = schema;
        this.path = Paths.get(filename);
        this.delimiter = delimiter;
        this.tuplesPerPage = tuplesPerPage;
    }

    @Override
    public void load() {
        // TODO: Implement
        try {
            this.lines = Files.readAllLines(path, java.nio.charset.StandardCharsets.UTF_8);
            this.table = new DBPAXpage[(int)Math.ceil((double)lines.size()/(double)tuplesPerPage)];
        } catch (IOException e) {
            e.printStackTrace();
        }
        Object[][] bycolumns = new Object[schema.length][lines.size()];
        for (int i = 0; i < lines.size(); i++) {
            int index = 0;
            for (Object j : lines.get(i).split(delimiter)) {
                bycolumns[index][i] = j;
                index++;
            }
        }

        int remainingRows = lines.size();
        int index = 0;
        int PAXPagesnumb = (int)Math.ceil((double)remainingRows / (double)tuplesPerPage);
        while (PAXPagesnumb != 0) { // number of PAX pages
            DBTuple[] minipages = new DBTuple[schema.length];
            int tuplesnumb = Math.min(tuplesPerPage, lines.size() - (tuplesPerPage*index));
            for (int mpnumb = 0; mpnumb < schema.length; mpnumb++) { // number of minipages (#attributes)
                Object[] fields = new Object[tuplesnumb];
                System.arraycopy(bycolumns[mpnumb], tuplesPerPage*index, fields, 0,  tuplesnumb);
                minipages[mpnumb] = new DBTuple(fields, Arrays.copyOfRange(schema, mpnumb, mpnumb+1));
            }
            this.table[index] = new DBPAXpage(minipages, schema, tuplesnumb);
            index++;
            PAXPagesnumb--;
        }

    }

    @Override
    public DBTuple getRow(int rownumber) {
        // TODO: Implement
        Object[] returnedFields = new Object[schema.length];
        int pagenumb = (int)Math.ceil(rownumber / tuplesPerPage);
        int fieldnumb = Math.max((rownumber % tuplesPerPage), 0);
//        for (int i = 0; i < this.table.length; i++) {
//            int tuplesnumb = Math.min(tuplesPerPage, lines.size() - (tuplesPerPage*i));
//            for (int a = 0; a < schema.length; a++) {
//                for (int j = 0; j < tuplesnumb; j++) {
//                    System.out.print(this.table[i].minipages[a].fields[j] + " ");
//                }
//                System.out.println();
//            }
//            System.out.println("-- end of minipages ---");
//        }
        for (int minipagenumb = 0; minipagenumb < schema.length; minipagenumb++) {
            returnedFields[minipagenumb] = this.table[pagenumb].minipages[minipagenumb].fields[fieldnumb];
        }

        return new DBTuple(returnedFields, schema);
    }
}
