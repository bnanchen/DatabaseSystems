package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class ColumnStore extends Store {
    
    // TODO: Add required structures
    private DBColumn table[];
    private Path path;
    private DataType[] schema;
    private String delimiter;
    private List<String> lines;
    
    public ColumnStore(DataType[] schema, String filename, String delimiter) {
        // TODO: Implement
        this.table = new DBColumn[schema.length];
        this.schema = schema;
        this.delimiter = delimiter;
        this.path = Paths.get(filename);
    }
    
    @Override
    public void load() {
        // TODO: Implement
        try {
            this.lines = Files.readAllLines(path, StandardCharsets.UTF_8);
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
//            for (int a = 0; a < schema.length; a++) {
//                System.out.print(bycolumns[a][i]);
//            }
//            System.out.println("");
        }
        for (int i = 0; i < schema.length; i++) {
            Object[] column = new Object[lines.size()];
            for (int j = 0; j < lines.size(); j++) {
                column[j] = bycolumns[i][j];
            }
//            for (int a = 0; a < lines.size(); a++) {
//                System.out.print(column[a]);
//            }
//            System.out.println("");
            this.table[i] = new DBColumn(column, schema[i]);
        }
    }
    
    @Override
    public DBColumn[] getColumns(int[] columnsToGet) {
        // TODO: Implement
        DBColumn[] columns = new DBColumn[columnsToGet.length];
        for (int i = 0; i < columnsToGet.length; i ++) {
            columns[0] = this.table[columnsToGet[i]];
        }
        return columns;
    }
}

