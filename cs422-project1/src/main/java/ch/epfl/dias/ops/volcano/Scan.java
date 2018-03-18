package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	// TODO: Add required structures
	private Store store;
	private int index;

	// usable with row and PAX
	public Scan(Store store) {
		// TODO: Implement
		this.store = store;
	}

	@Override
	public void open() {
		// TODO: Implement
        this.store.load();
        this.index = 0;
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
        DBTuple next = this.store.getRow(this.index);
        this.index++;
		return next;
	}

	@Override
	public void close() {
		// TODO: Implement
        this.index = 0;
	}
}