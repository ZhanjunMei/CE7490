# Simulation of Package-Aware Scheduling Algorithm 

This repository contains the simulation code for the Package-Aware Scheduling Algorithm (PASch), which is designed to optimize function execution in Function-as-a-Service (FaaS) platforms by leveraging package affinity, consistent hashing, and the power of two choices to improve cache reuse at worker nodes.

## Overview

Fast deployment and execution of cloud functions in FaaS platforms are critical for modern microservices architectures. PASch addresses the inefficiencies caused by function bloat due to large package dependencies, optimizing function execution and enhancing cache reuse.

## Key Features

- **Package Affinity**: PASch uses package affinity to route function requests to nodes that already have the required packages cached.
- **Consistent Hashing**: PASch integrates consistent hashing to accommodate changes in the number of worker nodes, ensuring minimal disruption to package assignments during autoscaling.
- **Power of Two Choices**: PASch employs the "power of two choices" technique to reduce the maximum load on any single server, leading to a more balanced distribution.


## Experiment Results

Our simulation experiments successfully reproduced some of the original effects of PASch. Additionally, we introduced a modified version of PASch (PASch-1) by replacing the power of two choices with a simpler one-choice mechanism, achieving comparable load balancing while further increasing package hit rates.

### Hit Rate

- **Least Loaded**: 16.03%
- **Hash Affinity**: 39.89%
- **PASch**: 36.04%

### Load Balance (Coefficient of Variation)

- **Least Loaded**: Best load balancing
- **Hash Affinity**: Worst load balancing
- **PASch**: Moderate load balancing


