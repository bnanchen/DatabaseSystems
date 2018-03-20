package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ProjectAggregate implements VectorOperator {

    // TODO: Add required structures
    private VectorOperator child;
    private Aggregate agg;
    private DataType dt;
    private int fieldNo;

    public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
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
    public DBColumn[] next() {
        // TODO: Implement
        DBColumn[] columns = child.next();
        Object result[] = new Object[1];

        switch (agg) {
            case AVG:
                int index = 0;
                if (dt == DataType.INT) {
                    int sumAvgInt = 0;
                    while (!columns[0].eof) {
                        Integer[] integerColumn = columns[fieldNo].getAsInteger();
                        int[] intColumn = new int[integerColumn.length];
                        for (int i = 0; i < integerColumn.length; i++) {
                            intColumn[i] = intColumn[i];
                            index++;
                        }
                        sumAvgInt += IntStream.of(intColumn).sum();
                        columns = child.next();
                    }
                    result[0] = sumAvgInt/index;
                } else if (dt == DataType.DOUBLE) {
                    double sumAvgDouble = 0.0;
                    while (!columns[0].eof) {
                        Double[] doubleColumn = columns[fieldNo].getAsDouble();
                        double[] coolDoubleColumn = new double[doubleColumn.length];
                        for (int i = 0; i < doubleColumn.length; i++) {
                            coolDoubleColumn[i] = doubleColumn[i];
                            index++;
                        }
                        sumAvgDouble += DoubleStream.of(coolDoubleColumn).sum();
                        columns = child.next();
                    }
                    result[0] = sumAvgDouble/index;
                }
                DBColumn[] resultAVG = {new DBColumn(result, dt)};
                return resultAVG;
            case MAX:
                double max = Double.MIN_VALUE;
                while(!columns[0].eof) {
                    Double[] doubleColumn = columns[fieldNo].getAsDouble();
                    double[] coolDoubleColumn = new double[doubleColumn.length+1];
                    for (int i = 0; i < doubleColumn.length; i++) {
                        coolDoubleColumn[i] = doubleColumn[i];
                    }
                    coolDoubleColumn[doubleColumn.length] = max;
                    max = DoubleStream.of(coolDoubleColumn).max().getAsDouble();
                    columns = child.next();
                }
                switch (dt) {
                    case INT:
                        result[0] = (int)max;
                        break;
                    case DOUBLE:
                        result[0] = max;
                        break;
                    default: throw new IllegalArgumentException();
                }
                DBColumn[] resultMAX = {new DBColumn(result, dt)};
                return resultMAX;
            case MIN:
                // TODO faire pour les strings
                double min = Double.MAX_VALUE;
                while(!columns[0].eof) {
                    Double[] doubleColumn = columns[fieldNo].getAsDouble();
                    double[] coolDoubleColumn = new double[doubleColumn.length+1];
                    for (int i = 0; i < doubleColumn.length; i++) {
                        coolDoubleColumn[i] = doubleColumn[i];
                    }
                    coolDoubleColumn[doubleColumn.length] = min;
                    min = DoubleStream.of(coolDoubleColumn).min().getAsDouble();
                    columns = child.next();
                }
                switch (dt) {
                    case INT:
                        result[0] = (int)min;
                        break;
                    case DOUBLE:
                        result[0] = min;
                        break;
                    default: throw new IllegalArgumentException();
                }
                DBColumn[] resultMIN = {new DBColumn(result, dt)};
                return resultMIN;
            case SUM:
                double sum = 0.0;
                while (!columns[0].eof) {
                    Double[] doubleColumn = columns[fieldNo].getAsDouble();
                    double[] coolDoubleColumn = new double[doubleColumn.length];
                    for (int i = 0; i < doubleColumn.length; i++) {
                        coolDoubleColumn[i] = doubleColumn[i];
                    }
                    sum += DoubleStream.of(coolDoubleColumn).sum();
                    columns = child.next();
                }
                switch (dt) {
                    case INT:
                        result[0] = (int)sum;
                        break;
                    case DOUBLE:
                        result[0] = sum;
                        break;
                    default: throw new IllegalArgumentException();
                }
                DBColumn[] resultSUM = {new DBColumn(result, dt)};
                return resultSUM;
            case COUNT:
                int count = 0;
                while (!columns[0].eof) {
                    Object[] doubleColumn = columns[fieldNo].column;
//                    System.out.println(doubleColumn.length);
                    count += (int)(long) Stream.of(doubleColumn).count();
                    columns = child.next();
                }
                result[0] = count;
                DBColumn[] resultCOUNT = {new DBColumn(result, dt)};
                return resultCOUNT;
        }
        return null;
    }

    @Override
    public void close() {
        // TODO: Implement
        child.close();
    }

}
