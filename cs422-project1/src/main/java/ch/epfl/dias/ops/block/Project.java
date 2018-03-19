package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.DBColumn;

public class Project implements BlockOperator {

	// TODO: Add required structures
	private BlockOperator child;
	private int[] columns;

	public Project(BlockOperator child, int[] columns) {
		// TODO: Implement
        this.child = child;
        this.columns = columns;
	}

	public DBColumn[] execute() {
		// TODO: Implement
        DBColumn[] columnsBlock = child.execute();
        DBColumn[] result = new DBColumn[columns.length];
        int index = 0;
        for (int colIndex : columns) {
            result[index] = columnsBlock[colIndex];
            index++;
        }
		return result;
	}
}
