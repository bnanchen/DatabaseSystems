package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ProjectAggregate implements BlockOperator {

	// TODO: Add required structures
    private BlockOperator child;
    private Aggregate agg;
    private DataType dt;
    private int fieldNo;
	
	public ProjectAggregate(BlockOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
        this.child = child;
        this.agg = agg;
        this.dt = dt;
        this.fieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement
        DBColumn[] columns = child.execute();
        Object result[] = new Object[1];

        switch (agg) {
            case AVG:
                Integer[] integerColumn = columns[fieldNo].getAsInteger();
                int[] intColumn = new int[integerColumn.length];
                for (int i = 0; i < integerColumn.length; i++) {
                    intColumn[i] = integerColumn[i];
                }
                int sum = IntStream.of(intColumn).sum();
                switch (dt) {
                    case INT:
                        result[0] = sum / integerColumn.length;
                        DBColumn[] resultColInt = {new DBColumn(result, dt)};
                        return resultColInt;
                    case DOUBLE:
                        result[0] = (double)sum / (double)integerColumn.length;
                        DBColumn[] resultColDouble = {new DBColumn(result, dt)};
                        return resultColDouble;
                    default: throw new IllegalArgumentException();
                }
            case MAX: case MIN:
                if (dt == DataType.INT) {
                    Integer[] integerColumnMax = columns[fieldNo].getAsInteger();
                    int[] intColumnMax = new int[integerColumnMax.length];
                    for (int i = 0; i < integerColumnMax.length; i++) {
                        intColumnMax[i] = integerColumnMax[i];
                    }
                    if (agg == Aggregate.MAX) {
                        result[0] = IntStream.of(intColumnMax).max();
                    }
                    result[0] = IntStream.of(intColumnMax).min();
                } else if (dt == DataType.DOUBLE) {
                    Double[] doubleColumnMax = columns[fieldNo].getAsDouble();
                    double[] cooldoubleColumnMax = new double[doubleColumnMax.length];
                    for (int i = 0; i < doubleColumnMax.length; i++) {
                        cooldoubleColumnMax[i] = doubleColumnMax[i];
                    }
                    if (agg == Aggregate.MAX) {
                        result[0] = DoubleStream.of(cooldoubleColumnMax).max();
                    }
                    result[0] = DoubleStream.of(cooldoubleColumnMax).min();
                } else if (dt == DataType.STRING) {
                    String[] stringColumn = columns[fieldNo].getAsString();
                    List<String> sorted = Arrays.asList(stringColumn);
                    Collections.sort(sorted);
                    if (agg == Aggregate.MAX) {
                        result[0] = sorted.get(sorted.size());
                    }
                    result[0] = sorted.get(0);
                }
                DBColumn[] resultMinMax = {new DBColumn(result, dt)};
                return resultMinMax;
            case SUM:
                Double[] doubleColumn = columns[fieldNo].getAsDouble();
                double[] coolDoubleColumn = new double[doubleColumn.length];
                for (int i = 0; i < doubleColumn.length; i++) {
                    coolDoubleColumn[i] = doubleColumn[i];
                }
                result[0] = DoubleStream.of(coolDoubleColumn).sum();
                DBColumn[] resultSum = {new DBColumn(result, dt)};
                return resultSum;
            case COUNT:
                result[0] = (int)(long)Stream.of(columns[fieldNo].column).count();
                DBColumn[] resultCount = {new DBColumn(result, dt)};
                return resultCount;
        }
		return null;
	}
}
