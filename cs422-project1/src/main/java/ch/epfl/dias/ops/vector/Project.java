package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VectorOperator {

	// TODO: Add required structures
	private VectorOperator child;
	private int[] fieldNo;

	public Project(VectorOperator child, int[] fieldNo) {
		// TODO: Implement
        this.child = child;
        this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
        child.open();
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
        DBColumn[] columns = child.next();
        DBColumn[] result = new DBColumn[fieldNo.length];
        int index = 0;
        for (int colIndex : fieldNo) {
            result[index] = columns[colIndex];
            index++;
        }
		return result;
	}

	@Override
	public void close() {
		// TODO: Implement
        child.close();
	}
}
