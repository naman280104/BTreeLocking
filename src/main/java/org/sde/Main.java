package org.sde;

import org.sde.celllevel.QueryRunnerCellLevel;
import org.sde.rowlevel.QueryRunnerRowLevel;


public class Main {

    public static void main(String [] args){

        int QUERY_COUNT = 7_000_000;
        int THREAD_COUNT_INSERT = 1;
        int THREAD_COUNT_MAX = 6;

        System.out.println("\nQuery Count: " + QUERY_COUNT);
        System.out.println("THREAD_COUNT_INSERT: " + THREAD_COUNT_INSERT);
        System.out.println("THREAD_COUNT_FOR_OTHER_OPERATIONS: " + THREAD_COUNT_MAX);
        System.out.println();

        QueryRunnerRowLevel queryRunnerRowLevel = new QueryRunnerRowLevel();
        QueryRunnerCellLevel queryRunnerCellLevel = new QueryRunnerCellLevel();

        Measure measure = new Measure();
        measure.metricRow(queryRunnerRowLevel, Queries.INSERT, QUERY_COUNT, THREAD_COUNT_INSERT);
        measure.metricCell(queryRunnerCellLevel, Queries.INSERT, QUERY_COUNT, THREAD_COUNT_INSERT);

        System.out.println();

        measure.metricRow(queryRunnerRowLevel, Queries.READ_PESSIMISTIC_ROW_LOCK, QUERY_COUNT, THREAD_COUNT_MAX);
        measure.metricCell(queryRunnerCellLevel, Queries.READ_PESSIMISTIC_CELL_LOCK, QUERY_COUNT, THREAD_COUNT_MAX);

        System.out.println();

        measure.metricRow(queryRunnerRowLevel, Queries.READ_OPTIMISTIC_ROW_LOCK, QUERY_COUNT, THREAD_COUNT_MAX);
        measure.metricCell(queryRunnerCellLevel, Queries.READ_OPTIMISTIC_CELL_LOCK, QUERY_COUNT, THREAD_COUNT_MAX);


        System.out.println();
        measure.metricRow(queryRunnerRowLevel, Queries.UPDATE_ROW_LOCK, QUERY_COUNT, THREAD_COUNT_MAX);
        measure.metricCell(queryRunnerCellLevel, Queries.UPDATE_CELL_LOCK, QUERY_COUNT, THREAD_COUNT_MAX);
    }
}


class Measure {

    public void metricRow(QueryRunnerRowLevel queryRunnerRowLevel, Queries queryType, int queryCount, int threadCount) {
        long startTime = System.nanoTime();
        queryRunnerRowLevel.runQuery(queryType, queryCount ,threadCount);
        long endTime = System.nanoTime();
        System.out.println("Metric-Row:  " + queryType.toString() + "  -- " + (endTime - startTime) / 1_000_000 + "ms");
    }

    public void metricCell(QueryRunnerCellLevel queryRunnerCellLevel, Queries queryType, int queryCount, int threadCount) {
        long startTime = System.nanoTime();
        queryRunnerCellLevel.runQuery(queryType, queryCount, threadCount);
        long endTime = System.nanoTime();
        System.out.println("Metric-Cell: " + queryType.toString() + " -- " + (endTime - startTime) / 1_000_000 + "ms");
    }
}
