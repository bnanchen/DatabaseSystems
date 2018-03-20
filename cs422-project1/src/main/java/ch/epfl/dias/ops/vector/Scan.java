package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import javax.sound.midi.SysexMessage;
import java.util.ArrayList;

public class Scan implements VectorOperator {

	// TODO: Add required structures
	private ColumnStore store;
	private int vectorSize;
	private int[] columnsToGet;
	private DBColumn[] columns;
	private int vectorNumber;
	private int remainingTuples;

	public Scan(Store store, int vectorSize) {
		// TODO: Implement
        this.store = (ColumnStore) store;
        this.vectorSize = vectorSize;
        this.vectorNumber = 0;
	}
	
	@Override
	public void open() {
		// TODO: Implement
        store.load();
        columnsToGet = new int[store.getNumberOfColumns()];
        for (int i = 0; i < store.getNumberOfColumns(); i++) {
            columnsToGet[i] = i;
        }
        columns = store.getColumns(columnsToGet);
        remainingTuples = columns[0].column.length;
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
        if (remainingTuples == 0) {
            DBColumn[] empty = {new DBColumn()};
            return empty;
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

//        for (int v = 0; v < vectorSize; v++) {
//            for (int col = 0; col < columns.length; col++) {
//                if (vectorSize*vectorNumber+v < columns[0].column.length) {
//                    tempResult.get(col).add(columns[col].column[vectorSize*vectorNumber+v]);
//                }
//            }
//        }
        vectorNumber++;

        DBColumn[] result = new DBColumn[columns.length];

        for (int i = 0; i < tempResult.size(); i++) {
            result[i] = new DBColumn(tempResult.get(i).toArray(), columns[i].type);
        }
//        System.out.println("------");
//        for (int i = 0; i < result[0].column.length; i++) {
//            System.out.println(result[0].column[i]);
//        }
		return result;
	}

	@Override
	public void close() {
		// TODO: Implement
        vectorSize = 0;
	}
}
