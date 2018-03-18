package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.Collections;

public class ProjectAggregate implements VolcanoOperator {

	// TODO: Add required structures
	private VolcanoOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;

	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
        this.child = child;
        this.agg = agg;
        this.dt = dt;
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
        DBTuple returnTuple = new DBTuple();
        DataType[] dtArray = {dt};
        Object result[] = new Object[1];
        switch (agg) {
            case AVG:
                int accumulator = 0;
                int index = 0;
                while (!next.eof) {
                    accumulator += next.getFieldAsInt(fieldNo);
                    index += 1;
                    next = child.next();
                }
                result[0] = accumulator/index;
                returnTuple =  new DBTuple(result, dtArray);
                break;
            case MAX: case MIN:
                ArrayList columns = new ArrayList<>();
                while(!next.eof) {
                    columns.add(next.fields[fieldNo]);
                    next = child.next();
                }
                if (agg == Aggregate.MAX) {
                    result[0] = Collections.max(columns); // TODO maybe doesn't work because Array of Objects
                } else {
                    result[0] = Collections.min(columns);
                }
                returnTuple = new DBTuple(result, dtArray);
                break;
            case SUM:
                double sum = 0;
                while(!next.eof) {
                    sum += next.getFieldAsDouble(fieldNo);
                    next = child.next();
                }
                result[0] = sum;
                returnTuple = new DBTuple(result, dtArray);
                break;
            case COUNT:
                int numb = 0;
                while (!next.eof) {
                    numb += 1;
                    next = child.next();
                }
                result[0] = numb;
                returnTuple = new DBTuple(result, dtArray);
                break;
        }
        return returnTuple;
	}

	@Override
	public void close() {
		// TODO: Implement
        child.close();
	}

}
