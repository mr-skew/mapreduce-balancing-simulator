# Mapreduce-balancing-simulator

Mapreduce-balancing-simulator is a tool for analyzing applicability of various load-balancing methods in countering data skew in MapReduce computations. A divisible load model of MapReduce is used for simulations.

The analyzed algorithms are: the reference algorithm, static(2), static(10), multi-dynamic, mixed(0.5), mixed(0.9), and divide-keys.


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


Sample input and output files can be found in /mr-balancing/sample directory.


## License

This project is released under the GPLv3 License. See the LICENSE file for details.
 