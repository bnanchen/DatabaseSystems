package ch.epfl.dias.store.row;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class RowStore extends Store {
    
    private DBTuple table[];
    private Path path;
    private DataType[] schema;
    private String delimiter;
    private List<String> lines;
    
    public RowStore(DataType[] schema, String filename, String delimiter) {
        this.path = Paths.get(filename);
        this.schema = schema;
        this.delimiter = delimiter;
    }
    
    @Override
    public void load() {
        try {
            this.lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.table = new DBTuple[lines.size()+1];
        for (int i = 0; i < lines.size(); i++) {
            String arr[] = lines.get(i).split(delimiter);
            table[i] = new DBTuple(castFill(arr), schema);
        }
        table[lines.size()] = new DBTuple();
    }

    // function to fill correctly
    private Object[] castFill(String[] arr) {
        Object[] castArr = new Object[arr.length];
        int index = 0;
        for (DataType dt : this.schema) {
            switch (dt) {
                case STRING:
                    castArr[index] = arr[index];
                    break;
                case BOOLEAN:
                    castArr[index] = Boolean.parseBoolean(arr[index]);
                    break;
                case DOUBLE:
                    castArr[index] = Double.parseDouble(arr[index]);
                    break;
                case INT:
                    castArr[index] = Integer.parseInt(arr[index]);
                    break;
            }
            index++;
        }
        return castArr;
    }
    
    @Override
    public DBTuple getRow(int rownumber) {
        return table[rownumber];
    }
}

