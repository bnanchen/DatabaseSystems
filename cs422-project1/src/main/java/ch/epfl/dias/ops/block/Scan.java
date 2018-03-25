package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements BlockOperator {

	private ColumnStore store;

	public Scan(ColumnStore store) {
        this.store = store;
	}

	@Override
	public DBColumn[] execute() {
        store.load();
        int[] columnsToGet = new int[store.getNumberOfColumns()];
        for (int i = 0; i < store.getNumberOfColumns(); i++) {
            columnsToGet[i] = i;
        }
        return store.getColumns(columnsToGet);
	}
}
