package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	// TODO: Add required structures
    private VolcanoOperator child;
    private int[] fieldNo;

	public Project(VolcanoOperator child, int[] fieldNo) {
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
	public DBTuple next() {
		// TODO: Implement
        DBTuple next = child.next();

        if (next.eof) {
            return new DBTuple();
        }
        DataType dt[] = new DataType[fieldNo.length];
        Object tuples[] = new Object[fieldNo.length];

        int index = 0;
        for (int no : fieldNo) {
            tuples[index] = next.fields[no];
            dt[index] = next.types[no];
            index++;
        }
		return new DBTuple(tuples, dt);
	}

	@Override
	public void close() {
		// TODO: Implement
        child.close();
	}
}
