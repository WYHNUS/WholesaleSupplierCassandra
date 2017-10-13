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
- Install [maven](https://stackoverflow.com/questions/7532928/how-do-i-install-maven-with-yum), 
and ensure `mvn --version` command works

### Setup

1. Open up Java IDE and import this project _(IntelliJ is used for this project, but other Java IDE should be similar)_.

2. Add Java8 and Datastax Java Driver as dependency for the project _(It is recommended to use Maven for convenience)_.

![screen shot for IntelliJ](/img/IntelliJ%20dep%20screenshot.png)

3. Start cassandra on local machine: `<path to cassandra folder>/cassandra -f`

4. In `/src/main/java/Setup.java` update variable values for `CONTACT_POINT` and `KEY_SPACE` 
to accommodate to your machine environment.

5. Create a `data` folder under the root directory if not present, 
put all bootstrap data files in `.csv` format into `data` folder.

6. In project root folder, compile the project via 
`mvn clean dependency:copy-dependencies package`

7. Run `java -cp target/*:target/dependency/*:. main.java.Setup` once 
to create key space and schemas used in this project. 
Meanwhile, all bootstrap data will be loaded into the local machine.

### Exectution

1. After following above setup steps, execute the main class for the project via 
`java -cp target/*:target/dependency/*:. main.java.Main`