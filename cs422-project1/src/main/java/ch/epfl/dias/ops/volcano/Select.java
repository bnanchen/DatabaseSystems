package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	// TODO: Add required structures
	private VolcanoOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
        this.child = child;
        this.op = op;
        this.fieldNo = fieldNo;
        this.value = value;
	}

	@Override
	public void open() {
		// TODO: Implement
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		return null;
	}

	@Override
	public void close() {
		// TODO: Implement
	}
}
