# Mapreduce-balancing-simulator

Mapreduce-balancing-simulator is a tool for analyzing applicability of various load-balancing methods in countering data skew in MapReduce computations. A divisible load model of MapReduce is used for simulations [1].

The analyzed algorithms are: the reference algorithm, static(2), static(10), multi-dynamic, mixed(0.5), mixed(0.9) [2], and divide-keys [3].


## References

1. J. Berliñska, M. Drozdowski, Scheduling divisible MapReduce computations, J. Parallel Distrib. Computing 71(3), 2011, 450–459,
[doi:10.1016/j.jpdc.2010.12.004](http://dx.doi.org/10.1016/j.jpdc.2010.12.004)

2. J. Berliñska, M. Drozdowski, Algorithms to Mitigate Partition Skew in MapReduce Applications, Research Report RA-01/15, Institute of Computing Science, Poznan Univ. of Technology, 2015,
http://www.cs.put.poznan.pl/mdrozdowski/rapIIn/RA-01-2015.pdf

3. J. Berliñska, M. Drozdowski, Comparing Load-Balancing Algorithms for MapReduce under Zipfian Data Skews, 2017, submitted.


## Building

The project can be built from Microsoft Visual Studio.

Open /mr-balancing/mr-balancing.sln in VS, and use Build Solution to build the source code.

Executable file mr-balancing.exe is produced in the /mr-balancing/Debug directory.


## Running mapreduce-balancing-simulator

Running mapreduce-balancing-simulator on input file sample.in, saving results in sample.out:

```
mr-balancing.exe sample.in > sample.out
```

The input file format is as follows:

- First the system/application parameters in the form: key value \n.

  Keys are {V, C, A, a_sort, a_red, a_master, comm_master_size, gamma, gamma1, m, r, bisection, key_num}.
  
  A key-value pair must start at the beginning of the line.
  
  Everything that follows after the key-value pair in the current line is ignored.
  
- Then, a sequence of key_num frequencies of the keys.


For each analyzed load-balancing method, the program reports the computed MapReduce execution time, two measures of reducer completion time dispersion (IQR and standard deviation), and a measure of load-balancing costs.


Sample input and output files can be found in the /mr-balancing/sample directory.


## License

This project is released under the GPLv3 License. See the LICENSE file for details.
 