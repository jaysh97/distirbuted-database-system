# distirbuted-database-system

**Advanced Database Sharding Implementations: SQL & NoSQL**


This repository provides a comparative analysis and hands-on implementation of database sharding strategies across both relational (SQL) and NoSQL paradigms. It serves as a practical demonstration of horizontal scaling, showcasing how to distribute data across multiple database instances to handle large-scale workloads.

The project contains two primary, parallel implementations:

SQL Sharding: A Java application that performs hash-based sharding on a relational schema using an in-memory H2 database cluster.

MongoDB Sharding: A Java application that leverages MongoDB's native sharding capabilities, inserting data into a sharded collection via a mongos query router.

**I. Conceptual Foundations of Sharding**
Sharding is a database architecture pattern for horizontal scaling. Instead of increasing the resources of a single server (vertical scaling), we partition a large dataset and distribute the subsets (shards) across multiple independent servers. Each shard is a self-contained database, yet the collection of shards functions as a single logical database.

Key Concepts:
Partitioning Key (Shard Key): The specific field or column used to determine how data is distributed. The choice of a shard key is critical; it must ensure even data distribution and support efficient query patterns to avoid "hotspots" (overloaded shards) and cross-shard queries. In this project, country is used as the shard key.

Query Routing: A router or coordinator process (e.g., mongos in MongoDB, or custom logic in the SQL implementation) that directs application queries to the appropriate shard(s) based on the shard key.

Benefits:

Scalability: Handles massive datasets and high throughput by distributing the load.

Performance: Queries targeting a specific shard key are faster as they operate on a smaller dataset.

Availability: Failure of one shard does not necessarily impact the availability of others (though it requires careful management).

**II. Project Structure & Setup**
Prerequisites
Java 8 or higher

Apache Maven

A running MongoDB sharded cluster (for the MongoDB implementation). This includes mongod shard servers, config servers, and at least one mongos query router.

Dependencies
The project is managed with Maven. All required dependencies are listed in the pom.xml file, including:

OpenCSV (for data parsing)

H2 Database (for in-memory SQL sharding)

MongoDB Java Driver

JUnit 5 (for testing)

Sample Data
A users.csv file provides the sample dataset for insertion. The applications will read from this file. A larger, 100-record version is also available to better demonstrate data distribution.

**III. SQL Sharding Implementation**
This implementation manually simulates a sharded relational database architecture using multiple H2 in-memory databases.

Sharding Strategy: Hash-Based
The application calculates the hash code of the country field from each CSV record.

A modulo operation (hashCode() % NUM_SHARDS) maps the hash to a specific shard ID.

This deterministic approach ensures that all records for the same country are co-located on the same shard, which is optimal for queries filtering by country.

**How to Run:**
Ensure users.csv is in the project's root directory.

Execute the main method in ShardedCsvInserter.java.

The application will:

Initialize a cluster of 3 in-memory H2 database shards.

Read the CSV file and calculate the target shard for each row.

Perform a batched INSERT into the appropriate shard.

Run a verification query on each shard to display the distributed data.

How to Test:
Run the ShardedCsvInserterTest.java class using Maven or an IDE. The test will:

Create a temporary CSV file.

Execute the sharding logic.

Connect to each H2 shard to assert that data was inserted and distributed correctly according to the hash-based sharding rule.

**IV. MongoDB Sharded Collection Implementation**
This implementation leverages MongoDB's built-in, production-ready sharding architecture.

Sharding Strategy: Hashed Shard Key
The application connects to a mongos query router, which is the entry point to the sharded cluster.

It programmatically issues commands to the admin database to:

Enable sharding for the user_db database.

Shard the users collection, specifying the country field as the shard key.

MongoDB's internal sharding mechanism then handles the data distribution across the available shards.

**How to Run:**
Crucial Prerequisite: Ensure your MongoDB sharded cluster is running and accessible.

Update the MONGO_URI constant in MongoCsvInserter.java to point to your mongos instance (e.g., mongodb://localhost:27017).

Ensure users.csv is in the project's root directory.

Execute the main method in MongoCsvInserter.java.

The application will perform an idempotent setup of the sharding configuration and then execute a bulk insertMany operation. The mongos router will automatically distribute the documents to the correct shards.

How to Test:
Run the MongoCsvInserterTest.java class. This is an integration test that will:

Connect to your running mongos instance.

Execute the main application logic.

Verify that the data was inserted correctly by querying the sharded collection through the mongos router.
