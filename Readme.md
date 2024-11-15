<!-- ## Getting Started

To run this project, you'll need to have the following prerequisites installed:

- Java Development Kit (JDK) version 21

### Running the Project

1. Clone the repository to your local machine.
2. Navigate to root folder of the repo.
3. mvn install
4. mvn package
5. java -Xmx12g -jar target/BTreeLocking-1.0-SNAPSHOT.jar
 -->


# Database Query Benchmarking System

## Overview
This project is designed to benchmark various database operations (INSERT, READ, UPDATE) using both row-level and cell-level locking mechanisms. It supports multi-threading, allowing the execution of queries concurrently to simulate different load scenarios and measure the performance of various query types under different thread configurations.

The main goal is to measure the execution time of these operations to assess the impact of concurrency and locking strategies on database performance.

## Features
- **Row-Level Locking**: Supports benchmarking of queries using row-level locking (both pessimistic and optimistic).
- **Cell-Level Locking**: Supports benchmarking of queries using cell-level locking (both pessimistic and optimistic).
- **Multi-threading**: Allows execution of queries with multiple threads, simulating real-world concurrent operations.
- **Customizable Configurations**: Ability to configure the number of queries, threads, and the types of queries for benchmarking.
- **Time Measurement**: Measures and prints the execution time of queries in milliseconds for performance analysis.

## Prerequisites
To run this project, you need the following software installed:

- **Java 8 or later**: Required to run the project.
- **Maven**: Used to manage project dependencies and build the project.

## Setup Instructions

### Clone the Repository
To get started, clone the repository to your local machine:

1. Clone the Repository
```bash
git clone <repository-url>
```

2. Navigate to root folder of the repo.
```bash 
cd <project-directory>
```

3. Install dependencies 
```bash
mvn clean install
```

4. Build the package
```bash
mvn package
```

5. Run the built jar file 
(Here -Xmx12g represents that 12GB is the max limit of JVM Heap Size.)
```bash
java -Xmx12g -jar target/BTreeLocking-1.0-SNAPSHOT.jar
```

