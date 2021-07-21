# Merger

## How to use it

### IntelliJ Idea or any other IDE 
1. Use -command=Merge -merge={directory_with_merge_files_path} -type={merge_type} as command line arguments. 
2. Run GraphBench.main(...)

### How to produce .jar file with stored procedures 

1. `mvn clean install -Pstored-procedures`

2. Copy produced `target/manta-stored-procedures-1.0-SNAPSHOT.jar` into neo4j plugins directory. 
(In config file it's set by: `dbms.directories.plugins`)  

3. In Neo4j configuration file set: `dbms.security.procedures.unrestricted=*`.

4. Restart Neo4j.
