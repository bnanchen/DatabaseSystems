package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public class ProjectAggregate implements VolcanoOperator {

	private VolcanoOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;

	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
        this.child = child;
        this.agg = agg;
        this.dt = dt;
        this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
        child.open();
	}

	@Override
	public DBTuple next() {
        DBTuple next = child.next();
        DBTuple returnTuple = new DBTuple();
        DataType[] dtArray = {dt};
        Object result[] = new Object[1];
        DataType fieldType = next.types[fieldNo];
        switch (agg) {
            case AVG:
                ArrayList avgArray = new ArrayList();
                while(!next.eof) {
                    avgArray.add(next.fields[fieldNo]);
                    next = child.next();
                }
                if (fieldType == DataType.INT) {
                    int[] avgInt = new int[avgArray.size()];
                    for (int i = 0; i < avgInt.length; i++) {
                        avgInt[i] = (int)avgArray.get(i);
                    }
                    int accumulator = IntStream.of(avgInt).sum();
                    if (dt == DataType.INT) {
                        result[0] = (int)accumulator/(int)avgInt.length;
                    } else if (dt == DataType.DOUBLE) {
                        result[0] = (double)accumulator/(double)avgInt.length;
                    }
                } else if (fieldType == DataType.DOUBLE) {
                    double[] avgDouble = new double[avgArray.size()];
                    for (int i = 0; i < avgDouble.length; i++) {
                        avgDouble[i] = (double)avgArray.get(i);
                    }
                    double accumulator = DoubleStream.of(avgDouble).sum();

                    if (dt == DataType.INT) {
                        result[0] = (int)accumulator/(int)avgDouble.length;
                    } else if (dt == DataType.DOUBLE) {
                        result[0] = (double)accumulator/(double)avgDouble.length;
                    }
                }
                returnTuple =  new DBTuple(result, dtArray);
                break;
            case MAX: case MIN:
                if (dt == DataType.STRING) {
                    ArrayList<String> columns = new ArrayList<>();
                    while(!next.eof) {
                        columns.add(next.getFieldAsString(fieldNo));
                        next = child.next();
                    }
                    for (int i = 0; i < columns.size(); i++) {
                        while(columns.get(i).charAt(0) == ' ') {
                            columns.set(i, removeCharAt(columns.get(i)));
                        }
                    }
                    if (agg == Aggregate.MIN) {
                        result[0] = Collections.min(columns);
                    } else {
                        result[0] = Collections.max(columns);
                    }

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
                ArrayList sumInt = new ArrayList();
                while(!next.eof) {
                        sumInt.add(next.fields[fieldNo]);
                    next = child.next();
                }

                if (dt == DataType.INT) {
                    int[] sumIntArray = new int[sumInt.size()];
                    for (int i = 0; i < sumInt.size(); i++) {
                        sumIntArray[i] = (int)sumInt.get(i);
                    }
                    result[0] = IntStream.of(sumIntArray).sum();
                } else {
                    double[] sumDoubleArray = new double[sumInt.size()];
                    for (int i = 0; i < sumInt.size(); i++) {
                        sumDoubleArray[i] = (double)sumInt.get(i);
                    }
                    result[0] = DoubleStream.of(sumDoubleArray).sum();
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

    private static String removeCharAt(String s) {
        return s.substring(0, 0) + s.substring(1);
    }

	@Override
	public void close() {
        child.close();
	}

}
