package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ProjectAggregate implements BlockOperator {

    private BlockOperator child;
    private Aggregate agg;
    private DataType dt;
    private int fieldNo;
	
	public ProjectAggregate(BlockOperator child, Aggregate agg, DataType dt, int fieldNo) {
        this.child = child;
        this.agg = agg;
        this.dt = dt;
        this.fieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute() {
        DBColumn[] columns = child.execute();
        Object result[] = new Object[1];

        switch (agg) {
            case AVG:
                if (columns[fieldNo].type == DataType.INT) {
                    Integer[] integerColumn = columns[fieldNo].getAsInteger();
                    int[] intColumn = new int[integerColumn.length];
                    for (int i = 0; i < integerColumn.length; i++) {
                        intColumn[i] = integerColumn[i];
                    }
                    int sum = IntStream.of(intColumn).sum();
                    if (dt == DataType.INT) {
                        result[0] = (int)sum / (int)integerColumn.length;
                    } else if (dt == DataType.DOUBLE) {
                        result[0] = (double)sum / (double)integerColumn.length;
                    }

                } else if (columns[fieldNo].type == DataType.DOUBLE) {
                    Double[] doubleColumn = columns[fieldNo].getAsDouble();
                    double[] coolDoubleColumn = new double[doubleColumn.length];
                    for (int i = 0; i < doubleColumn.length; i++) {
                        coolDoubleColumn[i] = doubleColumn[i];
                    }
                    double sum = DoubleStream.of(coolDoubleColumn).sum();
                    if (dt == DataType.INT) {
                        result[0] = (int)sum / (int)doubleColumn.length;
                    } else if (dt == DataType.DOUBLE) {
                        result[0] = (double)sum / (double)doubleColumn.length;
                    }
                }
                return new DBColumn[]{new DBColumn(result, dt)};
            case MAX: case MIN:
                switch (dt) {
                    case INT: {
                        Integer[] integerColumnMax = columns[fieldNo].getAsInteger();
                        ArrayList intColumnMax = new ArrayList();
                        Collections.addAll(intColumnMax, integerColumnMax);

                        if (agg == Aggregate.MAX) {
                            result[0] = Collections.max(intColumnMax);
                        } else {
                            result[0] = Collections.min(intColumnMax);
                        }
                        break;
                    }
                    case DOUBLE: {
                        Double[] integerColumnMax = columns[fieldNo].getAsDouble();
                        ArrayList doubleColumnMax = new ArrayList();
                        Collections.addAll(doubleColumnMax, integerColumnMax);

                        if (agg == Aggregate.MAX) {
                            result[0] = Collections.max(doubleColumnMax);
                        } else {
                            result[0] = Collections.min(doubleColumnMax);
                        }
                        break;
                    }
                    case STRING:
                        String[] stringColumn = columns[fieldNo].getAsString();
                        ArrayList<String> stringColumnMax = new ArrayList<>();
                        Collections.addAll(stringColumnMax, stringColumn);
                        for (int i = 0; i < stringColumnMax.size(); i++) {
                            while (stringColumnMax.get(i).charAt(0) == ' ') {
                                stringColumnMax.set(i, removeCharAt(stringColumnMax.get(i)));
                            }
                        }
                        Collections.sort(stringColumnMax);
                        if (agg == Aggregate.MAX) {
                            result[0] = stringColumnMax.get(stringColumnMax.size() - 1);
                        } else {
                            result[0] = stringColumnMax.get(0);
                        }
                        break;
                }
                return new DBColumn[]{new DBColumn(result, dt)};
            case SUM:
                if (dt == DataType.DOUBLE) {
                    Double[] doubleColumn = columns[fieldNo].getAsDouble();
                    double[] coolDoubleColumn = new double[doubleColumn.length];
                    for (int i = 0; i < doubleColumn.length; i++) {
                        coolDoubleColumn[i] = doubleColumn[i];
                    }
                    result[0] = DoubleStream.of(coolDoubleColumn).sum();
                } else if (dt == DataType.INT) {
                    Integer[] integerColumnSum = columns[fieldNo].getAsInteger();
                    int[] intColumnSum = new int[integerColumnSum.length];
                    for (int i = 0; i < integerColumnSum.length; i++) {
                        intColumnSum[i] = integerColumnSum[i];
                    }
                    result[0] = IntStream.of(intColumnSum).sum();
                }
                return new DBColumn[]{new DBColumn(result, dt)};
            case COUNT:
                result[0] = (int)(long)Stream.of(columns[fieldNo].column).count();
                return new DBColumn[]{new DBColumn(result, dt)};
        }
		return null;
	}

    private static String removeCharAt(String s) {
        return s.substring(0, 0) + s.substring(1);
    }
}
