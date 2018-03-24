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

import javax.xml.crypto.Data;

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
//                Object[] oui = castFill(fields, types[0]);
//                for (int i = 0; i < oui.length; i++) {
//                    System.out.println("bobo "+ oui[i]);
//                }
                minipages[mpnumb] = new DBTuple(castFill(fields, types[0]), types);
//                System.out.println(minipages[mpnumb].fields[0]);
            }
            this.table[index] = new DBPAXpage(minipages, schema, tuplesnumb);
//            System.out.println("index pax "+ index );
            index++;
            PAXPagesnumb--;
        }

//        System.out.println(table.length-1);
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
//        System.out.println("rownumber "+ rownumber);
        // TODO: Implement
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
//        System.out.println("pagenumb "+ pagenumb);
//        System.out.println("fieldnumb "+ fieldnumb);
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
//            System.out.println(this.table[pagenumb].minipages[minipagenumb].fields.length);
//            System.out.println(fieldnumb);
            returnedFields[minipagenumb] = this.table[pagenumb].minipages[minipagenumb].fields[fieldnumb];
//            System.out.println("caca "+ this.table[pagenumb].minipages[minipagenumb].fields[fieldnumb]);
        }
//        System.out.println("caca");
        return new DBTuple(returnedFields, schema);
    }
}
