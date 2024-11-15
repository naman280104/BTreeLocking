package org.sde.celllevel;
import org.sde.pair.Pair;

import java.util.ArrayList;
import java.util.concurrent.locks.StampedLock;


public class Person {
    private Pair<Integer, StampedLock> id;
    private Pair<String, StampedLock> name;
    private Pair<Integer, StampedLock> age;
    private Pair<Boolean, StampedLock> isMale;
    private Pair<Integer, StampedLock> bankBalance;
    private Pair<Integer, StampedLock> shares;
    private Pair<Boolean, StampedLock> isSelling;

    public Person(int id, String name, int age, boolean isMale) {
        this.id = new Pair<>(id, new StampedLock());
        this.name = new Pair<>(name, new StampedLock());
        this.age = new Pair<>(age, new StampedLock());
        this.isMale = new Pair<>(isMale, new StampedLock());
        this.bankBalance = new Pair<>(0, new StampedLock());
        this.shares = new Pair<>(0, new StampedLock());
        this.isSelling = new Pair<>(false, new StampedLock());
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id.getKey() +
                ", name='" + name.getKey() + '\'' +
                ", age=" + age.getKey() +
                ", isMale=" + isMale.getKey() +
                ", bankBalance=" + bankBalance.getKey() +
                ", shares=" + shares.getKey() +
                ", isSelling=" + isSelling.getKey() +
                '}';
    }

    public class UpdateBuilder {
        private Integer id;
        private String name;
        private Integer age;
        private Boolean isMale;
        private Integer bankBalance;
        private Integer shares;
        private Boolean isSelling;

        // Store the locks for the fields to be updated
        private ArrayList<StampedLock> locksToAcquire = new ArrayList<>();

        public UpdateBuilder setId(int id) {
            this.id = id;
            locksToAcquire.add(Person.this.id.getValue());
            return this;
        }

        public UpdateBuilder setName(String name) {
            this.name = name;
            locksToAcquire.add(Person.this.name.getValue());
            return this;
        }

        public UpdateBuilder setAge(int age) {
            this.age = age;
            locksToAcquire.add(Person.this.age.getValue());
            return this;
        }

        public UpdateBuilder setMale(boolean isMale) {
            this.isMale = isMale;
            locksToAcquire.add(Person.this.isMale.getValue());
            return this;
        }

        public UpdateBuilder setBankBalance(int bankBalance) {
            this.bankBalance = bankBalance;
            locksToAcquire.add(Person.this.bankBalance.getValue());
            return this;
        }

        public UpdateBuilder setShares(int shares) {
            this.shares = shares;
            locksToAcquire.add(Person.this.shares.getValue());
            return this;
        }

        public UpdateBuilder setIsSelling(boolean isSelling) {
            this.isSelling = isSelling;
            locksToAcquire.add(Person.this.isSelling.getValue());
            return this;
        }

        // Apply updates in a single write lock
        public void build() {
            // Acquire all locks for the fields we are updating
            ArrayList<Long> acquiredStamps = new ArrayList<>();
            try {
                // Acquire locks for all fields
                for (StampedLock lock : locksToAcquire) {
                    long stamp = lock.writeLock();
                    acquiredStamps.add(stamp);
                }

                // Perform the updates
                if (this.id != null) {
                    Person.this.id = new Pair<>(this.id, Person.this.id.getValue());
                }
                if (this.name != null) {
                    Person.this.name = new Pair<>(this.name, Person.this.name.getValue());
                }
                if (this.age != null) {
                    Person.this.age = new Pair<>(this.age, Person.this.age.getValue());
                }
                if (this.isMale != null) {
                    Person.this.isMale = new Pair<>(this.isMale, Person.this.isMale.getValue());
                }
                if (this.bankBalance != null) {
                    Person.this.bankBalance = new Pair<>(this.bankBalance, Person.this.bankBalance.getValue());
                }
                if (this.shares != null) {
                    Person.this.shares = new Pair<>(this.shares, Person.this.shares.getValue());
                }
                if (this.isSelling != null) {
                    Person.this.isSelling = new Pair<>(this.isSelling, Person.this.isSelling.getValue());
                }
            } finally {
                // Release all the locks in the reverse order they were acquired
                for (int i = acquiredStamps.size() - 1; i >= 0; i--) {
                    locksToAcquire.get(i).unlockWrite(acquiredStamps.get(i));
                }
            }
        }
    }

    // Provide an UpdateBuilder instance
    public UpdateBuilder update() {
        return new UpdateBuilder();
    }



    public class PessimisticReadBuilder {
        
        private final ArrayList<Pair<Object, StampedLock>> readFields = new ArrayList<>();

        private Integer id;
        private String name;
        private Integer age;
        private Boolean isMale;
        private Integer bankBalance;
        private Integer shares;
        private Boolean isSelling;



        public PessimisticReadBuilder readId() {
            readFields.add(new Pair<>(Person.this.id.getKey(), Person.this.id.getValue()));
            return this;
        }

        public PessimisticReadBuilder readName() {
            readFields.add(new Pair<>(Person.this.name.getKey(), Person.this.name.getValue()));
            return this;
        }

        public PessimisticReadBuilder readAge() {
            readFields.add(new Pair<>(Person.this.age.getKey(), Person.this.age.getValue()));
            return this;
        }

        public PessimisticReadBuilder readIsMale() {
            readFields.add(new Pair<>(Person.this.isMale.getKey(), Person.this.isMale.getValue()));
            return this;
        }

        public PessimisticReadBuilder readBankBalance() {
            readFields.add(new Pair<>(Person.this.bankBalance.getKey(), Person.this.bankBalance.getValue()));
            return this;
        }

        public PessimisticReadBuilder readShares() {
            readFields.add(new Pair<>(Person.this.shares.getKey(), Person.this.shares.getValue()));
            return this;
        }

        public PessimisticReadBuilder readIsSelling() {
            readFields.add(new Pair<>(Person.this.isSelling.getKey(), Person.this.isSelling.getValue()));
            return this;
        }

        public PessimisticReadBuilder build() {
            long[] stamps = new long[readFields.size()];
            try {
                for (int i = 0; i < readFields.size(); i++) {
                    Pair<Object, StampedLock> pair = readFields.get(i);
                    stamps[i] = pair.getValue().readLock();  // Acquire the lock
                        if (pair.equals(Person.this.id)) this.id = (Integer) pair.getKey();
                        else if (pair.equals(Person.this.age)) this.age = (Integer) pair.getKey();
                        else if (pair.equals(Person.this.bankBalance)) this.bankBalance = (Integer) pair.getKey();
                        else if (pair.equals(Person.this.shares)) this.shares = (Integer) pair.getKey();
                        else if(pair.equals(Person.this.name)) this.name = (String) pair.getKey();
                        else if (pair.equals(Person.this.isMale)) this.isMale = (Boolean) pair.getKey();
                        else if (pair.equals(Person.this.isSelling)) this.isSelling = (Boolean) pair.getKey();
                    }
            } finally {
                for (int i = readFields.size() - 1; i >= 0; i--) {
                    readFields.get(i).getValue().unlockRead(stamps[i]);
                }
            }
            return this;
        }

        // Getters for the selected fields
        public Integer getId() { return id; }
        public String getName() { return name; }
        public Integer getAge() { return age; }
        public Boolean getIsMale() { return isMale; }
        public Integer getBankBalance() { return bankBalance; }
        public Integer getShares() { return shares; }
        public Boolean getIsSelling() { return isSelling; }
    }

    // Provide an PessimisticReadBuilder instance
    public PessimisticReadBuilder readPessimistic() {
        return new PessimisticReadBuilder();
    }

    public class OptimisticReadBuilder {
        private final ArrayList<Pair<Object, StampedLock>> readFields = new ArrayList<>();
        private Integer id;
        private String name;
        private Integer age;
        private Boolean isMale;
        private Integer bankBalance;
        private Integer shares;
        private Boolean isSelling;

        public OptimisticReadBuilder readId() {
            readFields.add(new Pair<>(Person.this.id.getKey(), Person.this.id.getValue()));
            return this;
        }

        public OptimisticReadBuilder readName() {
            readFields.add(new Pair<>(Person.this.name.getKey(), Person.this.name.getValue()));
            return this;
        }

        public OptimisticReadBuilder readAge() {
            readFields.add(new Pair<>(Person.this.age.getKey(), Person.this.age.getValue()));
            return this;
        }

        public OptimisticReadBuilder readIsMale() {
            readFields.add(new Pair<>(Person.this.isMale.getKey(), Person.this.isMale.getValue()));
            return this;
        }

        public OptimisticReadBuilder readBankBalance() {
            readFields.add(new Pair<>(Person.this.bankBalance.getKey(), Person.this.bankBalance.getValue()));
            return this;
        }

        public OptimisticReadBuilder readShares() {
            readFields.add(new Pair<>(Person.this.shares.getKey(), Person.this.shares.getValue()));
            return this;
        }

        public OptimisticReadBuilder readIsSelling() {
            readFields.add(new Pair<>(Person.this.isSelling.getKey(), Person.this.isSelling.getValue()));
            return this;
        }

        public OptimisticReadBuilder build() {
            boolean isValid = true;
            do{
                long[] stamps = new long[readFields.size()];

                try {
                    for (int i = 0; i < readFields.size(); i++) {
                        Pair<Object, StampedLock> pair = readFields.get(i);
                        stamps[i] = pair.getValue().tryOptimisticRead();  // Acquire the lock
                        if (pair.equals(Person.this.id)) this.id = (Integer) pair.getKey();
                        else if (pair.equals(Person.this.age)) this.age = (Integer) pair.getKey();
                        else if (pair.equals(Person.this.bankBalance)) this.bankBalance = (Integer) pair.getKey();
                        else if (pair.equals(Person.this.shares)) this.shares = (Integer) pair.getKey();
                        else if(pair.equals(Person.this.name)) this.name = (String) pair.getKey();
                        else if (pair.equals(Person.this.isMale)) this.isMale = (Boolean) pair.getKey();
                        else if (pair.equals(Person.this.isSelling)) this.isSelling = (Boolean) pair.getKey();
                    }
                } finally {
                    // Unlock all fields in reverse order
                    for (int i = readFields.size() - 1; i >= 0; i--) {
                        if(readFields.get(i).getValue().validate(stamps[i])) continue;
                        else {
                            isValid = false;
                            break;
                        }
                    }
                }
            } while(!isValid);

            return this;
        }

        // Getters for the selected fields
        public Integer getId() { return id; }
        public String getName() { return name; }
        public Integer getAge() { return age; }
        public Boolean getIsMale() { return isMale; }
        public Integer getBankBalance() { return bankBalance; }
        public Integer getShares() { return shares; }
        public Boolean getIsSelling() { return isSelling; }

    }

    public OptimisticReadBuilder readOptimistic() {
        return new OptimisticReadBuilder();
    }

}
