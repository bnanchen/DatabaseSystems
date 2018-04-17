package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

    private VolcanoOperator child;
    private int[] fieldNo;

	public Project(VolcanoOperator child, int[] fieldNo) {
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
        child.open();
	}

	@Override
	public DBTuple next() {
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
        child.close();
	}
}
