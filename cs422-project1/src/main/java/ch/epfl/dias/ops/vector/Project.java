package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.column.DBColumn;

public class Project implements VectorOperator {

	private VectorOperator child;
	private int[] fieldNo;

	public Project(VectorOperator child, int[] fieldNo) {
        this.child = child;
        this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
        child.open();
	}

	@Override
	public DBColumn[] next() {
        DBColumn[] columns = child.next();
        DBColumn[] result = new DBColumn[fieldNo.length];
        if (columns[0].eof) {
            return new DBColumn[]{new DBColumn()};
        }
        int index = 0;
        for (int colIndex : fieldNo) {
        	System.out.println(colIndex);
            result[index] = columns[colIndex];
            index++;
        }
		return result;
	}

	@Override
	public void close() {
        child.close();
	}
}
