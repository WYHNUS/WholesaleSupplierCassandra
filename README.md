# Wholesale Supplier Application

### Introduction

This project is made for module CS4224 Distributed Databases from National University of Singapore.

The aim for this project is to acquire practical experience with using distributed database systems 
for application development.

In this project, an application for a wholesale supplier will be developed using 
[Apache Cassandra](http://cassandra.apache.org/).

Language used: [Java](http://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html)

### Prerequisite

- Download and install [Java 8](http://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html)
- Download and install [Apache Cassandra 3.11.0](http://www.apache.org/dyn/closer.lua/cassandra/3.11.0/apache-cassandra-3.11.0-bin.tar.gz)
- Download [Datastax Java Driver 3.3.0](http://docs.datastax.com/en/developer/java-driver/3.3/#getting-the-driver)

### Setup

1. Open up Java IDE and import this project _(IntelliJ is used for this project, but other Java IDE should be similar)_.
2. Add Java8 and Datastax Java Driver as dependency for the project.
![alt text](https://drive.google.com/file/d/0B0CdpMhgfkDyRTBFOWllN2szMmM/view?usp=sharing)
3. Start cassandra on local machine: `<path to cassandra folder>/cassandra -f`
4. In `src/Setup.java` update variable values for `CONTACT_POINT` and `KEY_SPACE` 
to accommodate to your machine environment.
5. Run `src/Setup.java` once to create key space and schemas used in this project.
