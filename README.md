<img src="docs/fmi.svg" width="100%">

# FaaS Message Interface
Serverless platforms provide massive parallelism with very high elasticity and fine-grained billing. Because of these properties, they are increasingly used for stateful, distributed jobs at large scales. However, a major limitation of the commonly used platforms is communication: Individual functions cannot communicate directly and using external storage or databases for ephemeral data can be slow and expensive. We present FMI, the FaaS Message Interface, to overcome this limitation. FMI is an easy-to-use, high-performance framework for general-purpose communication in Function as a Service platforms. It supports different communication channels (including direct communication with our TCP NAT hole punching system), a model-driven channel selection according to performance or cost, and provides optimized collective implementations that exploit characteristics of the different channels.
In our experiments, FMI can speed up communication for a distributed machine learning job by up to 1,200x, while reducing cost at the same time by factors of up to 365. It provides a simple interface and can be integrated into existing codebases with a few minor changes.

If you use FMI in your work, then please cite our [ACM ICS 2023 paper](https://spcl.inf.ethz.ch/Publications/index.php?pub=459):

```
@inproceedings{10.1145/3577193.3593718,
author = {Copik, Marcin and B\"{o}hringer, Roman and Calotoiu, Alexandru and Hoefler, Torsten},
title = {FMI: Fast and Cheap Message Passing for Serverless Functions},
year = {2023},
isbn = {9798400700569},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
url = {https://doi.org/10.1145/3577193.3593718},
doi = {10.1145/3577193.3593718},
abstract = {Serverless functions provide elastic scaling and a fine-grained billing model, making Function-as-a-Service (FaaS) an attractive programming model. However, for distributed jobs that benefit from large-scale and dynamic parallelism, the lack of fast and cheap communication is a major limitation. Individual functions cannot communicate directly, group operations do not exist, and users resort to manual implementations of storage-based communication. This results in communication times multiple orders of magnitude slower than those found in HPC systems. We overcome this limitation and present the FaaS Message Interface (FMI). FMI is an easy-to-use, high-performance framework for general-purpose point-to-point and collective communication in FaaS applications. We support different communication channels and offer a model-driven channel selection according to performance and cost expectations. We model the interface after MPI and show that message passing can be integrated into serverless applications with minor changes, providing portable communication closer to that offered by high-performance systems. In our experiments, FMI can speed up communication for a distributed machine learning FaaS application by up to 162x, while simultaneously reducing cost by up to 397 times.},
booktitle = {Proceedings of the 37th International Conference on Supercomputing},
pages = {373–385},
numpages = {13},
keywords = {high-performance computing, I/O, serverless, function-as-a-service, faas},
location = {Orlando, FL, USA},
series = {ICS '23}
}
```

## Dependencies

- C++17 or higher
- Boost
- AWS SDK for C++
- hiredis
- [TCPunch](https://github.com/OpenCoreCH/TCPunch)

## Installation (C++)
- Clone this repository
- Add to your CMakeLists.txt:
```cmake
add_subdirectory(path_to_repo/FMI/)

target_link_libraries(${PROJECT_NAME} PRIVATE FMI)
target_include_directories(${PROJECT_NAME} PRIVATE ${FMI_INCLUDE_DIRS})
```
- Integrate the library into your project:
```cpp
#include <Communicator.h>
...
FMI::Communicator comm(peer_id, num_peers, "config/fmi.json", "MyApp", 512);
```

## Installation (Python)
- Clone this repository
```shell
cd python
mkdir build
cd build
cmake ..
make
```
- `fmi.so` gets created in the `python/build` directory. You can copy it into your Python module path or include the build directory via `PYTHONPATH`. The library can then be integrated into your project:
```python
import fmi
comm = fmi.Communicator(peer_id, num_peers, "config/fmi.json", "MyApp", 512);
```

### Docker Images
The Docker images [FMI-build-docker](https://github.com/OpenCoreCH/FMI-build-docker) contain all necessary dependencies and set up the environment for you. See the repo for details.

### AWS Lambda Layer
For even easier deployment, we provide AWS CloudFormation templates to create Lambda layers in [python/aws](python/aws). Simply run `sam build` and `sam deploy --guided` in the folder corresponding to your Python version, which creates a Lambda layer in your account that can be added to your function. As soon as you added the layer, you can simply use `import fmi` and work with the library.

## Examples
C++ sample code for the library is available at [tests/communicator.cpp](tests/communicator.cpp), the usage from Python is demonstrated in [python/tests/client.py](python/tests/client.py). 

## Documentation

The architecture of the system, including a comparison with existing systems and benchmarks, is documented in the ACM ICS'23 paper [FMI: Fast and Cheap Message Passing for Serverless Functions](https://spcl.inf.ethz.ch/Publications/.pdf/2023_ics_fmi.pdf). More details can be found in the thesis [FMI: The FaaS Message Interface](https://doi.org/10.3929/ethz-b-000532425).

A technical documentation of the system (for people that want to extend it) is available at [fmi.opencore.ch](https://fmi.opencore.ch).

## HPC for free (TODO...)

## Authors

* [Marcin Copik (ETH Zurich)](https://github.com/mcopik/) - maintainer.
* [Roman Böhringer (OpenCoreCH)](https://github.com/OpenCoreCH) - main author of FMI.
