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
    
    // TODO: Add required structures
    private DBTuple table[];
    private Path path;
    private DataType[] schema;
    private String delimiter;
    private List<String> lines;
    
    public RowStore(DataType[] schema, String filename, String delimiter) {
        // TODO: Implement
        this.path = Paths.get(filename);
        this.schema = schema;
        this.delimiter = delimiter;
    }
    
    @Override
    public void load() {
        // TODO: Implement
        try {
            this.lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.table = new DBTuple[lines.size()];
        for (int i = 0; i < lines.size(); i++) {
            table[i] = new DBTuple(lines.get(i).split(delimiter), schema);
        }
    }
    
    @Override
    public DBTuple getRow(int rownumber) {
        // TODO: Implement
        return table[rownumber];
    }
}

