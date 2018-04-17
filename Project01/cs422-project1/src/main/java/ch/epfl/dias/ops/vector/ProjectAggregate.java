package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ProjectAggregate implements VectorOperator {

    private VectorOperator child;
    private Aggregate agg;
    private DataType dt;
    private int fieldNo;

    public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
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
    public DBColumn[] next() {
        DBColumn[] columns = child.next();
        if (columns[0].eof) {
            return new DBColumn[]{new DBColumn()};
        }
        Object result[] = new Object[1];

        switch (agg) {
            case AVG:
                int index = 0;
                if (columns[fieldNo].type == DataType.INT) {
                    int sumAvgInt = 0;
                    while (!columns[0].eof) {
                        Integer[] integerColumn = columns[fieldNo].getAsInteger();
                        int[] intColumn = new int[integerColumn.length];
                        for (int i = 0; i < integerColumn.length; i++) {
                            intColumn[i] = integerColumn[i];
                            index++;
                        }
                        sumAvgInt += IntStream.of(intColumn).sum();
                        columns = child.next();
                    }
                    if (dt == DataType.INT) {
                        result[0] = (int)sumAvgInt/(int)index;
                    } else if (dt == DataType.DOUBLE) {
                        result[0] = (double)sumAvgInt/(double)index;
                    }

                } else if (columns[fieldNo].type == DataType.DOUBLE) {
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
                    if (dt == DataType.INT) {
                        result[0] = (int)sumAvgDouble/(int)index;
                    } else if (dt == DataType.DOUBLE) {
                        result[0] = (double)sumAvgDouble/(double)index;
                    }
                }
                return new DBColumn[]{new DBColumn(result, dt)};
            case MAX:
                double maxDouble = Double.MIN_VALUE;
                int maxInt = Integer.MIN_VALUE;
                switch (dt) {
                    case DOUBLE:
                        while (!columns[0].eof) {
                            Double[] doubleColumn = columns[fieldNo].getAsDouble();
                            ArrayList<Double> coolDoubleColumn = new ArrayList<>();
                            for (Double aDoubleColumn : doubleColumn) {
                                coolDoubleColumn.add((double) aDoubleColumn);
                            }
                            coolDoubleColumn.add(maxDouble);
                            maxDouble = Collections.max(coolDoubleColumn);
                            columns = child.next();
                        }
                        switch (dt) {
                            case INT:
                                result[0] = (int) maxDouble;
                                break;
                            case DOUBLE:
                                result[0] = maxDouble;
                                break;
                            default:
                                throw new IllegalArgumentException();
                        }
                        break;
                    case INT:
                        while (!columns[0].eof) {
                            Integer[] integerColumn = columns[fieldNo].getAsInteger();
                            ArrayList<Integer> intColumn = new ArrayList<>();
                            for (Integer anIntegerColumn : integerColumn) {
                                intColumn.add((int) anIntegerColumn);
                            }
                            intColumn.add(maxInt);
                            maxInt = Collections.max(intColumn);
                            columns = child.next();
                        }
                        switch (dt) {
                            case INT:
                                result[0] = (int) maxInt;
                                break;
                            case DOUBLE:
                                result[0] = maxInt;
                                break;
                            default:
                                throw new IllegalArgumentException();
                        }
                        break;
                    case STRING:
                        ArrayList<String> toSort = new ArrayList<>();
                        while (!columns[0].eof) {
                            toSort.addAll(Arrays.asList(columns[fieldNo].getAsString()));
                            columns = child.next();
                        }
                        for (int i = 0; i < toSort.size(); i++) {
                            while (toSort.get(i).charAt(0) == ' ') {
                                toSort.set(i, removeCharAt(toSort.get(i)));
                            }
                        }
                        result[0] = Collections.max(toSort);
                        break;
                }
                return new DBColumn[]{new DBColumn(result, dt)};
            case MIN:
                double minDouble = Double.MAX_VALUE;
                int minInt = Integer.MAX_VALUE;
                switch (dt) {
                    case DOUBLE:
                        while (!columns[0].eof) {
                            Double[] doubleColumn = columns[fieldNo].getAsDouble();
                            ArrayList<Double> coolDoubleColumn = new ArrayList<>();
                            for (Double aDoubleColumn : doubleColumn) {
                                coolDoubleColumn.add((double) aDoubleColumn);
                            }
                            coolDoubleColumn.add(minDouble);
                            minDouble = Collections.min(coolDoubleColumn);
                            columns = child.next();
                        }
                        switch (dt) {
                            case INT:
                                result[0] = (int) minDouble;
                                break;
                            case DOUBLE:
                                result[0] = minDouble;
                                break;
                            default:
                                throw new IllegalArgumentException();
                        }
                        break;
                    case INT:
                        while (!columns[0].eof) {
                            Integer[] integerColumn = columns[fieldNo].getAsInteger();
                            ArrayList<Integer> intColumn = new ArrayList<>();
                            for (Integer anIntegerColumn : integerColumn) {
                                intColumn.add((int) anIntegerColumn);
                            }
                            intColumn.add(minInt);
                            minInt = Collections.min(intColumn);
                            columns = child.next();
                        }
                        switch (dt) {
                            case INT:
                                result[0] = (int) minInt;
                                break;
                            case DOUBLE:
                                result[0] = minInt;
                                break;
                            default:
                                throw new IllegalArgumentException();
                        }
                        break;
                    case STRING:
                        ArrayList<String> toSort = new ArrayList<>();
                        while (!columns[0].eof) {
                            toSort.addAll(Arrays.asList(columns[fieldNo].getAsString()));
                            columns = child.next();
                        }
                        for (int i = 0; i < toSort.size(); i++) {
                            while (toSort.get(i).charAt(0) == ' ') {
                                toSort.set(i, removeCharAt(toSort.get(i)));
                            }
                        }
                        result[0] = Collections.min(toSort);
                        break;
                }
                return new DBColumn[]{new DBColumn(result, dt)};
            case SUM:
                double sum = 0.0;
                if (dt == DataType.INT) {
                    while (!columns[0].eof) {
                        Integer[] integerColumn = columns[fieldNo].getAsInteger();
                        int[] intColumn = new int[integerColumn.length];
                        for (int i = 0; i < integerColumn.length; i++) {
                            intColumn[i] = integerColumn[i];
                        }
                        sum += IntStream.of(intColumn).sum();
                        columns = child.next();
                    }

                } else if (dt == DataType.DOUBLE) {
                    while (!columns[0].eof) {
                        Double[] doubleColumn = columns[fieldNo].getAsDouble();
                        double[] coolDoubleColumn = new double[doubleColumn.length];
                        for (int i = 0; i < doubleColumn.length; i++) {
                            coolDoubleColumn[i] = doubleColumn[i];
                        }
                        sum += DoubleStream.of(coolDoubleColumn).sum();
                        columns = child.next();
                    }

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
                return new DBColumn[]{new DBColumn(result, dt)};
            case COUNT:
                int count = 0;
                while (!columns[0].eof) {
                    Object[] doubleColumn = columns[fieldNo].column;
                    count += (int)(long) Stream.of(doubleColumn).count();
                    columns = child.next();
                }
                result[0] = count;
                return new DBColumn[]{new DBColumn(result, dt)};
        }
        return null;
    }

    private static String removeCharAt(String s) {
        return s.substring(0, 0) + s.substring(1);
    }

    @Override
    public void close() {
        child.close();
    }

}
