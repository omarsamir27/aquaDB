# AQUAdb

## Single-User Relational Database Management System

### Authors

- [@Abouelhassen](https://github.com/Abouelhassen)

- [@AbdallahAloush](https://github.com/AbdallahAloush)

- [@AhmedOsamaElShafei](https://github.com/AhmedOsamaElShafei)

- [@omarsamir27](https://github.com/omarsamir27)

### Abstract

This is the project repo for our senior project requirement for a Bachelor's degree in computer and communications engineering at the Faculty of Engineering , Alexandria University.

This project is an oppurtunity to experiement with database design , develop complex systems software and get our hands dirty with Rust

### How to Use

The following instructions assume that you have `cargo` and `rustc` installed on your machine.

#### Running the server for the first time:

When running the server for the first time on your machine, it needs to initialize its directories and catalogues. This is done using the following command.

```bash
cargo run --package aquaDB --bin aquaDB -- init
```

This will initialize the catalogue and `AQUA` folder in your home directory.

### Running the Server:

If you previously ran the server on your machine, you don't need the `-- init` command in when running. So,  the command will be as follows.

```bash
cargo run --package aquaDB --bin aquaDB 
```

### Running the Client:

Use the following command

```bash
cargo run --package aquaDB --bin aqua-client
```

### Create a new database:

This project is based on SQL but here is a guide for some basic functionalities.

use `create db aqua` replace `aqua` with any name.

This creates a new database.

#### Connect to an existing database:

```bash
connect db aqua
```

#### Create a new table

```bash
create table student (id int, firstname varchar, lastname varchar , create index btree s_id on (id))
```

This creates a new table with name `student` that has the fields: id, firstname, lastname and has a BTREE index on id.



## Project Progress

- [x] Storage Engine
  
  - [x] Buffer Manager
  
  - [x] Block Manager
  
  - [x] Heap Pages
  
  - [x] Tuple Format

- [x] Indexing
  
  - [x] Hash Index
  
  - [x] B+Tree

- [x] Query Execution
  
  - [x] Table Creation 
  
  - [x] Table Scan
  
  - [x] Table Sorting
  
  - [x] Minimal WHERE clauses
  
  - [x] Record Insertion
  
  - [x] Record Deletion
  
  - [x] Joins

- [x] Parser

- [x] Planner

- [x] Query Plan Optimizer

- [x] Client side interface
