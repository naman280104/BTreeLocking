package org.sde.celllevel;


import org.sde.Queries;
import org.sde.database.BTree;

import java.util.ArrayList;

public class QueryRunnerCellLevel {
    private final BTree<Person> bTree = new BTree<>();

    private int QueryCount = 10_000;

    void queryInsert() {
        for (int i=0; i<QueryCount; i++) {
            Person p = new Person(i, "person"+i, 5*i, true);
            bTree.insert(i, p);
        }
    }

    void queryReadCellLock(String cellName) {
        switch (cellName){
            case "shares": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    Person.PessimisticReadBuilder pessimisticReadBuilder = p.readPessimistic().readShares().build();
                }
                break;
            }
            case "isSelling": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    Person.PessimisticReadBuilder pessimisticReadBuilder = p.readPessimistic().readIsSelling().build();
                }
                break;
            }
            case "age": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    Person.PessimisticReadBuilder pessimisticReadBuilder = p.readPessimistic().readAge().build();
                }
            }
            case "name": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    Person.PessimisticReadBuilder pessimisticReadBuilder = p.readPessimistic().readName().build();
                }
            }
            case "bankBalance": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    Person.PessimisticReadBuilder pessimisticReadBuilder = p.readPessimistic().readBankBalance().build();
                }
            }
            case "male": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    Person.PessimisticReadBuilder pessimisticReadBuilder = p.readPessimistic().readIsMale().build();
                }
            }
        }
    }

    

    void queryReadOptimisticCellLock(String cellName) {
        switch (cellName){
            case "shares": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    Person.OptimisticReadBuilder optimisticReadBuilder = p.readOptimistic().readShares().build();
                }
                break;
            }
            case "isSelling": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    Person.OptimisticReadBuilder optimisticReadBuilder = p.readOptimistic().readIsSelling().build();
                }
                break;
            }
            case "age": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    Person.OptimisticReadBuilder optimisticReadBuilder = p.readOptimistic().readAge().build();
                }
            }
            case "name": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    Person.OptimisticReadBuilder optimisticReadBuilder = p.readOptimistic().readName().build();
                }
            }
            case "bankBalance": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    Person.OptimisticReadBuilder optimisticReadBuilder = p.readOptimistic().readBankBalance().build();
                }
            }
            case "male": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    Person.OptimisticReadBuilder optimisticReadBuilder = p.readOptimistic().readIsMale().build();
                }
            }
        }
    }

    void queryUpdateCellLock(String cellName) {

        switch (cellName){
            case "shares": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    p.update().setShares(i).build();
                }
                break;
            }
            case "isSelling": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    p.update().setIsSelling(true).build();
                }
                break;
            }
            case "age": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    p.update().setAge(20).build();
                }
            }
            case "name": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    p.update().setName("Person"+i).build();
                }
                break;
            }
            case "bankBalance": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    p.update().setBankBalance(i*10).build();
                }
                break;
            }
            case "male": {
                for (int i=0; i<QueryCount; i++) {
                    Person p = bTree.searchIterative(0);
                    p.update().setMale(true).build();
                }
                break;
            }
        }
    }
    public void runQuery(Queries q, int queryCount, int threadCount) {

        if (queryCount != -1) QueryCount = queryCount;

        if (q == Queries.INSERT) queryInsert();
        else if (q == Queries.READ_PESSIMISTIC_CELL_LOCK) {
            ArrayList<Thread> threads = new ArrayList<>();
            ArrayList<String> cellNames = new ArrayList<>();

            cellNames.add("shares");
            cellNames.add("isSelling");
            cellNames.add("age");
            cellNames.add("name");
            cellNames.add("bankBalance");
            cellNames.add("male");

            for (int i=0; i<threadCount; i++) {
                int finalI = i;
                Thread t = new Thread(new Runnable() {
                    private final String cellName = cellNames.get(finalI);
                    @Override
                    public void run() {
                        queryReadCellLock(cellName);
                    }
                });
                threads.add(t);
            }
            for (Thread t : threads) {
                t.start();
            }

            try {
                for (Thread t : threads) {
                    t.join();
                }
            } catch (Exception e) {
                System.err.println("Thread execution was interrupted: " + e.getMessage());
            }
        }
        else if (q == Queries.READ_OPTIMISTIC_CELL_LOCK) {
            ArrayList<Thread> threads = new ArrayList<>();
            ArrayList<String> cellNames = new ArrayList<>();

            cellNames.add("shares");
            cellNames.add("isSelling");
            cellNames.add("age");
            cellNames.add("name");
            cellNames.add("bankBalance");
            cellNames.add("male");

            for (int i=0; i<threadCount; i++) {
                int finalI = i;
                Thread t = new Thread(new Runnable() {
                    private final String cellName = cellNames.get(finalI);
                    @Override
                    public void run() {
                        queryReadOptimisticCellLock(cellName);
                    }
                });
                threads.add(t);
            }
            for (Thread t : threads) {
                t.start();
            }

            try {
                for (Thread t : threads) {
                    t.join();
                }
            } catch (Exception e) {
                System.err.println("Thread execution was interrupted: " + e.getMessage());
            }
        }
        else if (q == Queries.UPDATE_CELL_LOCK) {

            ArrayList<Thread> threads = new ArrayList<>();
            ArrayList<String> cellNames = new ArrayList<>();

            cellNames.add("shares");
            cellNames.add("isSelling");
            cellNames.add("age");
            cellNames.add("name");
            cellNames.add("bankBalance");
            cellNames.add("male");

            for (int i=0; i<threadCount; i++) {
                int finalI = i;
                Thread t = new Thread(new Runnable() {
                    private final String cellName = cellNames.get(finalI);
                    @Override
                    public void run() {
                        queryUpdateCellLock(cellName);
                    }
                });
                threads.add(t);
            }
            for (Thread t : threads) {
                t.start();
            }

            try {
                for (Thread t : threads) {
                    t.join();
                }
            } catch (Exception e) {
                System.err.println("Thread execution was interrupted: " + e.getMessage());
            }

        }
    }
}
