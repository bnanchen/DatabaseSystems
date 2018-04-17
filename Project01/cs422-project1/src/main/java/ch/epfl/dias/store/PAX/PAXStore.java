package ch.epfl.dias.store.PAX;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class PAXStore extends Store {

    private DBPAXpage table[];
    private Path path;
    private DataType[] schema;
    private String delimiter;
    private int tuplesPerPage;
    private List<String> lines;

    public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
        this.schema = schema;
        this.path = Paths.get(filename);
        this.delimiter = delimiter;
        this.tuplesPerPage = tuplesPerPage;
    }

    @Override
    public void load() {
        try {
            this.lines = Files.readAllLines(path, java.nio.charset.StandardCharsets.UTF_8);
            this.table = new DBPAXpage[(int)Math.ceil((double)lines.size()/(double)tuplesPerPage)+1];
        } catch (IOException e) {
            e.printStackTrace();
        }
        String[][] bycolumns = new String[schema.length][lines.size()];
        for (int i = 0; i < lines.size(); i++) {
            int index = 0;
            for (String j : lines.get(i).split(delimiter)) {
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
                String[] fields = new String[tuplesnumb];
                System.arraycopy(bycolumns[mpnumb], tuplesPerPage*index, fields, 0,  tuplesnumb);
                DataType[] types = Arrays.copyOfRange(schema, mpnumb, mpnumb+1);
                minipages[mpnumb] = new DBTuple(castFill(fields, types[0]), types);
            }
            this.table[index] = new DBPAXpage(minipages, schema, tuplesnumb);
            index++;
            PAXPagesnumb--;
        }

        this.table[table.length-1] = new DBPAXpage();

    }

    private Object[] castFill(String[] arr, DataType dt) {
        Object[] castArr = new Object[arr.length];
        for (int i = 0; i < arr.length; i++) {
            switch (dt) {
                case STRING:
                    castArr[i] = arr[i];
                    break;
                case BOOLEAN:
                    castArr[i] = Boolean.parseBoolean(arr[i]);
                    break;
                case DOUBLE:
                    castArr[i] = Double.parseDouble(arr[i]);
                    break;
                case INT:
                    castArr[i] = Integer.parseInt(arr[i]);
                    break;
            }
        }
        return castArr;
    }

    @Override
    public DBTuple getRow(int rownumber) {
        Object[] returnedFields = new Object[schema.length];
        int pagenumb = (int)Math.ceil(rownumber / tuplesPerPage);
        int fieldnumb = Math.max(rownumber % tuplesPerPage, 0); // row

        if (this.table[pagenumb].minipages[0].eof) {
            return new DBTuple();
        }
        try {
            Object test = this.table[pagenumb].minipages[0].fields[fieldnumb];
        } catch (java.lang.ArrayIndexOutOfBoundsException e) {
            return new DBTuple();
        }
        for (int minipagenumb = 0; minipagenumb < schema.length; minipagenumb++) {
            returnedFields[minipagenumb] = this.table[pagenumb].minipages[minipagenumb].fields[fieldnumb];
        }
        return new DBTuple(returnedFields, schema);
    }
}
