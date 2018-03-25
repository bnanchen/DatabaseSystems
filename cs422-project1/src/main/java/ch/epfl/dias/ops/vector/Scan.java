package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;

public class Scan implements VectorOperator {

	private ColumnStore store;
	private int vectorSize;
	private DBColumn[] columns;
	private int vectorNumber;
	private int remainingTuples;

	public Scan(Store store, int vectorSize) {
        this.store = (ColumnStore) store;
        this.vectorSize = vectorSize;
        this.vectorNumber = 0;
	}
	
	@Override
	public void open() {
        store.load();
        int[] columnsToGet = new int[store.getNumberOfColumns()];
        for (int i = 0; i < store.getNumberOfColumns(); i++) {
            columnsToGet[i] = i;
        }
        columns = store.getColumns(columnsToGet);
        remainingTuples = columns[0].column.length;
	}

	@Override
	public DBColumn[] next() {
        if (remainingTuples == 0) {
            return new DBColumn[]{new DBColumn()};
        }

        ArrayList<ArrayList<Object>> tempResult = new ArrayList<>();
        for (int i = 0; i < store.getNumberOfColumns(); i++) {
            ArrayList<Object> temp = new ArrayList<>();
            tempResult.add(temp);
        }

        int currentVectorSize = 0;
        while (remainingTuples > 0 && currentVectorSize < vectorSize) {
            for (int col = 0; col < columns.length; col++) {
                tempResult.get(col).add(columns[col].column[vectorSize*vectorNumber+currentVectorSize]);

            }
            currentVectorSize++;
            remainingTuples--;
        }
        vectorNumber++;

        DBColumn[] result = new DBColumn[columns.length];

        for (int i = 0; i < tempResult.size(); i++) {
            result[i] = new DBColumn(tempResult.get(i).toArray(), columns[i].type);
        }
		return result;
	}

	@Override
	public void close() {
        vectorSize = 0;
	}
}
