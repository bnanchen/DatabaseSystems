package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.DBColumn;

public class Project implements BlockOperator {

	private BlockOperator child;
	private int[] columns;

	public Project(BlockOperator child, int[] columns) {
        this.child = child;
        this.columns = columns;
	}

	public DBColumn[] execute() {
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
