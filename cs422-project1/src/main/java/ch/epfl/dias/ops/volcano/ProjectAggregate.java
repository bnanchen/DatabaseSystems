package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
                switch (dt) {
                    case INT:
                        result[0] = (int)accumulator/(int)index;
                        break;
                    case DOUBLE:
                        result[0] = (double)accumulator/(double)index;
                        break;
                        default: throw new IllegalArgumentException();
                }
                returnTuple =  new DBTuple(result, dtArray);
                break;
            case MAX: case MIN:
                if (dt == DataType.STRING) {
                    ArrayList<String> columns = new ArrayList<String>();
                    while(!next.eof) {
                        columns.add(next.getFieldAsString(fieldNo));
                        next = child.next();
                    }
                    for (int i = 0; i < columns.size(); i++) {
                        while(columns.get(i).charAt(0) == ' ') {
                            columns.set(i, removeCharAt(columns.get(i), 0));
                        }
                    }
                    Collections.sort(columns); // TODO j'étais là
                    System.out.println("yououou "+ columns.get(0).charAt(0));
                    System.out.println(columns);
                    result[0] = columns.get(0);
                }
                else {
                    ArrayList columns = new ArrayList<>();
                    while(!next.eof) {
                        columns.add(next.fields[fieldNo]);
                        next = child.next();
                    }
                    if (agg == Aggregate.MAX) {
                        result[0] = Collections.max(columns);
                    } else {
                        result[0] = Collections.min(columns);
                    }
                }
                returnTuple = new DBTuple(result, dtArray);
                break;
            case SUM:
                int sumInt = 0;
                double sumDouble = 0.0;
                while(!next.eof) {
                    if (dt == DataType.INT) {
                        sumInt += next.getFieldAsInt(fieldNo);
                    } else {
                        sumDouble += next.getFieldAsDouble(fieldNo);
                    }
                    next = child.next();
                }
                if (dt == DataType.INT) {
                    result[0] = sumInt;
                } else {
                    result[0] = sumDouble;
                }
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

    private static String removeCharAt(String s, int pos) {
        return s.substring(0, pos) + s.substring(pos + 1);
    }

	@Override
	public void close() {
		// TODO: Implement
        child.close();
	}

}
