package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements BlockOperator {

	// TODO: Add required structures
	private ColumnStore store;

	public Scan(ColumnStore store) {
		// TODO: Implement
        this.store = store;
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement
        store.load();
        int[] columnsToGet = new int[store.getNumberOfColumns()];
        for (int i = 0; i < store.getNumberOfColumns(); i++) {
            columnsToGet[i] = i;
        }
        return store.getColumns(columnsToGet);
	}
}
