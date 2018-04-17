package ch.epfl.dias.store.column;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class ColumnStore extends Store {
    
    private DBColumn table[];
    private Path path;
    private DataType[] schema;
    private String delimiter;
    private List<String> lines;
    
    public ColumnStore(DataType[] schema, String filename, String delimiter) {
        this.table = new DBColumn[schema.length];
        this.schema = schema;
        this.delimiter = delimiter;
        this.path = Paths.get(filename);
    }
    
    @Override
    public void load() {
        try {
            this.lines = Files.readAllLines(path, StandardCharsets.UTF_8);
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
        for (int i = 0; i < schema.length; i++) {
            String[] column = new String[lines.size()];
            System.arraycopy(bycolumns[i], 0, column, 0, lines.size());
            this.table[i] = new DBColumn(castFill(column, i), schema[i]);
        }
    }

    // function to fill correctly
    private Object[] castFill(String[] arr, int dtIndex) {
        Object[] castArr = new Object[arr.length];
            switch (schema[dtIndex]) {
                case STRING:
                    castArr = arr;
                    break;
                case BOOLEAN:
                    for (int i = 0; i < arr.length; i++) {
                        castArr[i] = Boolean.parseBoolean(arr[i]);
                    }
                    break;
                case DOUBLE:
                    for (int i = 0; i < arr.length; i++) {
                        castArr[i] = Double.parseDouble(arr[i]);
                    }
                    break;
                case INT:
                    for (int i = 0; i < arr.length; i++) {
                        castArr[i] = Integer.parseInt(arr[i]);
                    }
                    break;
            }
        return castArr;
    }

    public int getNumberOfColumns() {
        return table.length;
    }
    
    @Override
    public DBColumn[] getColumns(int[] columnsToGet) {
        DBColumn[] columns = new DBColumn[columnsToGet.length];
        for (int i = 0; i < columnsToGet.length; i ++) {
            columns[i] = this.table[columnsToGet[i]];
        }
        return columns;
    }
}

