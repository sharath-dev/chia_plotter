# Chia-Plotter Implementation
## To Build the program
```cargo build -r```
then use the command
```cp target/releases/chia_plotter .```
to copy the compiled binary to the root folder.

## To Run a single benchmark
```./chia_plotter -k 24 -t 7 -f data -m 4096 | tee -a "$FILENAME"```
where ```k```is the plot size, ```t``` is the number of tables, ```f``` is the name of the file and ```m``` is the memory_limit.

## To Build and Run All Benchmarks
```
./run_benchmarks.sh
```