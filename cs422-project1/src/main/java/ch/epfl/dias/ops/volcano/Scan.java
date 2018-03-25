package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	private Store store;
	private int index;

	// usable with row and PAX
	public Scan(Store store) {
		this.store = store;
	}

	@Override
	public void open() {
        this.store.load();
        this.index = 0;
	}

	@Override
	public DBTuple next() {
        DBTuple next = this.store.getRow(this.index);
        this.index++;
		return next;
	}

	@Override
	public void close() {
        this.index = 0;
	}
}