package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {

    // TODO: Implement
    public Object[] column;
    public DataType type;
    public boolean eof;

    // represent one column
    public DBColumn(Object[] column, DataType type) {
        this.column = column;
        this.type = type;
        this.eof = false;
    }

    public DBColumn() {
        this.eof = true;
    }

    public Integer[] getAsInteger() {
        // TODO: Implement
        Integer columnInt[] = new Integer[column.length];
        for (int i = 0; i < column.length; i++) {
            columnInt[i] = (Integer)column[i];
        }
        return columnInt;
    }

    public Double[] getAsDouble() {
        Double columnDouble[] = new Double[column.length];
        for (int i = 0; i < column.length; i++) {
            columnDouble[i] = (Double)column[i];
        }
        return columnDouble;
    }

    public Boolean[] getAsBoolean() {
        Boolean columnBool[] = new Boolean[column.length];
        for (int i = 0; i < column.length; i++) {
            columnBool[i] = (Boolean)column[i];
        }
        return columnBool;
    }

    public String[] getAsString() {
        String columnString[] = new String[column.length];
        for (int i = 0; i < column.length; i++) {
            columnString[i] = (String)column[i];
        }
        return columnString;
    }
}
