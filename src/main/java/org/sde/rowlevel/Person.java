package org.sde.rowlevel;

import java.util.concurrent.locks.StampedLock;

public class Person {
    Integer id;
    String name;
    Integer age;
    boolean isMale;
    Integer bankBalance;
    Integer shares;
    boolean isSelling;
    StampedLock lock;

    public Person(int id, String name, int age, boolean isMale) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.isMale = isMale;
        this.bankBalance = 0;
        this.shares = 0;
        this.isSelling = false;
        lock = new StampedLock();

    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", isMale=" + isMale +
                ", bankBalance=" + bankBalance +
                ", shares=" + shares +
                ", isSelling=" + isSelling +
//                ", lock=" + lock +
                '}';
    }

    // Builder for selectively updating fields
    public class UpdateBuilder {
        private Integer id;
        private String name;
        private Integer age;
        private Boolean isMale;
        private Integer bankBalance;
        private Integer shares;
        private Boolean isSelling;

        public UpdateBuilder setId(int id) {
            this.id = id;
            return this;
        }

        public UpdateBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public UpdateBuilder setAge(int age) {
            this.age = age;
            return this;
        }

        public UpdateBuilder setMale(boolean isMale) {
            this.isMale = isMale;
            return this;
        }

        public UpdateBuilder setBankBalance(int bankBalance) {
            this.bankBalance = bankBalance;
            return this;
        }

        public UpdateBuilder setShares(int shares) {
            this.shares = shares;
            return this;
        }

        public UpdateBuilder setIsSelling(boolean isSelling) {
            this.isSelling = isSelling;
            return this;
        }

        // Apply updates in a single write lock
        public void build() {
            long stamp = lock.writeLock();
            try {
                if (this.id != null) {
                    Person.this.id = this.id;
                }
                if (this.name != null) {
                    Person.this.name = this.name;
                }
                if (this.age != null) {
                    Person.this.age = this.age;
                }
                if (this.isMale != null) {
                    Person.this.isMale = this.isMale;
                }
                if (this.bankBalance != null) {
                    Person.this.bankBalance = this.bankBalance;
                }
                if (this.shares != null) {
                    Person.this.shares = this.shares;
                }
                if (this.isSelling != null) {
                    Person.this.isSelling = this.isSelling;
                }
            } finally {
                lock.unlockWrite(stamp);
            }
        }
    }

    // Provide an UpdateBuilder instance
    public UpdateBuilder update() {
        return new UpdateBuilder();
    }


    // Inner class for selective reading
    public class PessimisticReadBuilder {
        private boolean readId = false;
        private boolean readName = false;
        private boolean readAge = false;
        private boolean readIsMale = false;
        private boolean readBankBalance = false;
        private boolean readShares = false;
        private boolean readIsSelling = false;

        private Integer id;
        private String name;
        private Integer age;
        private Boolean isMale;
        private Integer bankBalance;
        private Integer shares;
        private Boolean isSelling;

        public PessimisticReadBuilder readId() {
            this.readId = true;
            return this;
        }

        public PessimisticReadBuilder readName() {
            this.readName = true;
            return this;
        }

        public PessimisticReadBuilder readAge() {
            this.readAge = true;
            return this;
        }

        public PessimisticReadBuilder readIsMale() {
            this.readIsMale = true;
            return this;
        }

        public PessimisticReadBuilder readBankBalance() {
            this.readBankBalance = true;
            return this;
        }

        public PessimisticReadBuilder readShares() {
            this.readShares = true;
            return this;
        }

        public PessimisticReadBuilder readIsSelling() {
            this.readIsSelling = true;
            return this;
        }

        // Apply reads under a single read lock
        public PessimisticReadBuilder build() {
            long stamp = lock.readLock();
            try {
                if (readId) {
                    this.id = Person.this.id;
                }
                if (readName) {
                    this.name = Person.this.name;
                }
                if (readAge) {
                    this.age = Person.this.age;
                }
                if (readIsMale) {
                    this.isMale = Person.this.isMale;
                }
                if (readBankBalance) {
                    this.bankBalance = Person.this.bankBalance;
                }
                if (readShares) {
                    this.shares = Person.this.shares;
                }
                if (readIsSelling) {
                    this.isSelling = Person.this.isSelling;
                }
                
            } finally {
                lock.unlockRead(stamp);
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

    // Provide an instance of ReadBuilder
    public PessimisticReadBuilder readPessimistic() {
        return new PessimisticReadBuilder();
    }

    public class OptimisticReadBuilder {
        private boolean readId = false;
        private boolean readName = false;
        private boolean readAge = false;
        private boolean readIsMale = false;
        private boolean readBankBalance = false;
        private boolean readShares = false;
        private boolean readIsSelling = false;

        private Integer id;
        private String name;
        private Integer age;
        private Boolean isMale;
        private Integer bankBalance;
        private Integer shares;
        private Boolean isSelling;

        public OptimisticReadBuilder readId() {
            this.readId = true;
            return this;
        }

        public OptimisticReadBuilder readName() {
            this.readName = true;
            return this;
        }

        public OptimisticReadBuilder readAge() {
            this.readAge = true;
            return this;
        }

        public OptimisticReadBuilder readIsMale() {
            this.readIsMale = true;
            return this;
        }

        public OptimisticReadBuilder readBankBalance() {
            this.readBankBalance = true;
            return this;
        }

        public OptimisticReadBuilder readShares() {
            this.readShares = true;
            return this;
        }

        public OptimisticReadBuilder readIsSelling() {
            this.readIsSelling = true;
            return this;
        }

        // Apply reads under a single optimistic read lock with retry
        public OptimisticReadBuilder build() {
            boolean valid;
            do {
                long stamp = lock.tryOptimisticRead();

                // Optimistically read the fields
                if (readId) {
                    this.id = Person.this.id;
                }
                if (readName) {
                    this.name = Person.this.name;
                }
                if (readAge) {
                    this.age = Person.this.age;
                }
                if (readIsMale) {
                    this.isMale = Person.this.isMale;
                }
                if (readBankBalance) {
                    this.bankBalance = Person.this.bankBalance;
                }
                if (readShares) {
                    this.shares = Person.this.shares;
                }
                if (readIsSelling) {
                    this.isSelling = Person.this.isSelling;
                }

                // Validate the stamp after reading the fields
                valid = lock.validate(stamp);

                // If validation fails, retry, otherwise continue with successful read
            } while (!valid); // Retry if the optimistic read failed

            return this; // Return the builder with successfully read values
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

    // Provide an instance of ReadBuilder
    public OptimisticReadBuilder readOptimistic() {
        return new OptimisticReadBuilder();
    }






//
//    // Pessimistic Row Level --------------------------------  Pessimistic Row Level
//    long pessimisticRowLevelLock;
//
//    public int getIdPessimistic() {
//        pessimisticRowLevelLock = lock.readLock();
//        try {
//            return id;
//        } finally {
//            lock.unlockRead(pessimisticRowLevelLock);
//        }
//    }
//
//    public String getNamePessimistic() {
//        pessimisticRowLevelLock = lock.readLock(); // Acquire the read lock
//        try {
//            return name; // Return the value while holding the read lock
//        } finally {
//            lock.unlockRead(pessimisticRowLevelLock);
//        }
//    }
//
//    public int getAgePessimistic() {
//        pessimisticRowLevelLock = lock.readLock();
//        try {
//            return age;
//        }
//        finally {
//            lock.unlockRead(pessimisticRowLevelLock);
//        }
//    }
//
//    public boolean getIsMalePessimistic() {
//        pessimisticRowLevelLock = lock.readLock();
//        try {
//            return isMale;
//        }
//        finally {
//            lock.unlockRead(pessimisticRowLevelLock);
//        }
//    }
//
//    public int getBankBalancePessimistic() {
//        pessimisticRowLevelLock = lock.readLock();
//        try {
//            return bankBalance;
//        }
//        finally {
//            lock.unlockRead(pessimisticRowLevelLock);
//        }
//    }
//
//    public int getSharesPessimistic() {
//        pessimisticRowLevelLock = lock.readLock();
//        try {
//            return shares;
//        }
//        finally {
//            lock.unlockRead(pessimisticRowLevelLock);
//        }
//    }
//
//    public boolean getIsSellingPessimistic() {
//        pessimisticRowLevelLock = lock.readLock();
//        try {
//            return isSelling;
//        }
//        finally {
//            lock.unlockRead(pessimisticRowLevelLock);
//        }
//    }

//    public void setIsSellingPessimistic(boolean selling) {
//        pessimisticRowLevelLock = lock.writeLock();
//        isSelling = selling;
//        lock.unlockWrite(pessimisticRowLevelLock);
//    }
//
//    public void setIdPessimistic(int id) {
//        pessimisticRowLevelLock = lock.writeLock();
//        this.id = id;
//        lock.unlockWrite(pessimisticRowLevelLock);
//    }
//
//    public void setNamePessimistic(String name) {
//        pessimisticRowLevelLock = lock.writeLock();
//        this.name = name;
//        lock.unlockWrite(pessimisticRowLevelLock);
//    }
//
//    public void setAgePessimistic(int age) {
//        pessimisticRowLevelLock = lock.writeLock();
//        this.age = age;
//        lock.unlockWrite(pessimisticRowLevelLock);
//    }
//
//    public void setMalePessimistic(boolean male) {
//        pessimisticRowLevelLock = lock.writeLock();
//        isMale = male;
//        lock.unlockWrite(pessimisticRowLevelLock);
//    }
//
//    public void setBankBalancePessimistic(int bankBalance) {
//        pessimisticRowLevelLock = lock.writeLock();
//        this.bankBalance = bankBalance;
//        lock.unlockWrite(pessimisticRowLevelLock);
//    }
//
//    public void setSharesPessimistic(int shares) {
//        pessimisticRowLevelLock = lock.writeLock();
//        this.shares = shares;
//        lock.unlockWrite(pessimisticRowLevelLock);
//    }
//     Pessimistic Row Level --------------------------------  Pessimistic Row Level


//
//
//    // Optimistic Row Level  --------------------------------  Optimistic Row Level
//    long optimisticRowLevelLock;
//
//    public int getIdOptimistic() {
//        optimisticRowLevelLock = lock.readLock();
//        try {
//            return id;
//        } finally {
//            lock.unlockRead(optimisticRowLevelLock);
//        }
//    }
//
//    public String getNameOptimistic() {
//        optimisticRowLevelLock = lock.readLock(); // Acquire the read lock
//        try {
//            return name; // Return the value while holding the read lock
//        } finally {
//            lock.unlockRead(optimisticRowLevelLock);
//        }
//    }
//
//    public int getAgeOptimistic() {
//        optimisticRowLevelLock = lock.readLock();
//        try {
//            return age;
//        }
//        finally {
//            lock.unlockRead(optimisticRowLevelLock);
//        }
//    }
//
//    public boolean getIsMaleOptimistic() {
//        optimisticRowLevelLock = lock.readLock();
//        try {
//            return isMale;
//        }
//        finally {
//            lock.unlockRead(optimisticRowLevelLock);
//        }
//    }
//
//    public int getBankBalanceOptimistic() {
//        optimisticRowLevelLock = lock.readLock();
//        try {
//            return bankBalance;
//        }
//        finally {
//            lock.unlockRead(optimisticRowLevelLock);
//        }
//    }
//
//    public int getSharesOptimistic() {
//        optimisticRowLevelLock = lock.readLock();
//        try {
//            return shares;
//        }
//        finally {
//            lock.unlockRead(optimisticRowLevelLock);
//        }
//    }
//
//    public boolean getIsSellingOptimistic() {
//        optimisticRowLevelLock = lock.readLock();
//        try {
//            return isSelling;
//        }
//        finally {
//            lock.unlockRead(optimisticRowLevelLock);
//        }
//    }
//
//    public void setIsSellingOptimistic(boolean selling) {
//        optimisticRowLevelLock = lock.writeLock();
//        isSelling = selling;
//        lock.unlockWrite(optimisticRowLevelLock);
//    }
//
//    public void setIdOptimistic(int id) {
//        optimisticRowLevelLock = lock.writeLock();
//        this.id = id;
//        lock.unlockWrite(optimisticRowLevelLock);
//    }
//
//    public void setNameOptimistic(String name) {
//        optimisticRowLevelLock = lock.writeLock();
//        this.name = name;
//        lock.unlockWrite(optimisticRowLevelLock);
//    }
//
//    public void setAgeOptimistic(int age) {
//        optimisticRowLevelLock = lock.writeLock();
//        this.age = age;
//        lock.unlockWrite(optimisticRowLevelLock);
//    }
//
//    public void setMaleOptimistic(boolean male) {
//        optimisticRowLevelLock = lock.writeLock();
//        isMale = male;
//        lock.unlockWrite(optimisticRowLevelLock);
//    }
//
//    public void setBankBalanceOptimistic(int bankBalance) {
//        optimisticRowLevelLock = lock.writeLock();
//        this.bankBalance = bankBalance;
//        lock.unlockWrite(optimisticRowLevelLock);
//    }
//
//    public void setSharesOptimistic(int shares) {
//        optimisticRowLevelLock = lock.writeLock();
//        this.shares = shares;
//        lock.unlockWrite(optimisticRowLevelLock);
//    }
//


}
