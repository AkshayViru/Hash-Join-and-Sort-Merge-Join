# Hash-Join-and-Sort-Merge-Join
Implementation of database relations join operators - Sort Merge Join and Hash Join

## Implementation
Given M memory blocks and two large relations R(X,Y)and S(Y,Z). There are iterators for the following operations.
- SortMerge Join
  - open()- Create sorted sublists for R and S, each of size M blocks.
  - getnext()- Use 1 block for each sublist and get minimum of R & S. Joins this minimum Y value with the other table and return. Checks for B(R)+B(S)<M^2
  - close()- close all files
- Hash Join
  - open()- Create M1 hashed sublists for R and S
  - getnext()- For each Ri and Si thus created, loads the smaller of the two in the main memory and creates a search structure over it. Then recursively loads the other file in the remaining blocks and for each record of this file, search corresponding records (with same join attribute value) from the other file. Checks for min(B(R),B(S))<M^2
  - close()- close all files
  - 
Join condition is (R.Y==S.Y). One block is used for output which is filled by row returned by getnext() and when it gets full, appends it to the output file and continues.

## Usage

### Input Parameters
1. Path to file containing relation R
2. Path to file containing relation S
3. Type of join sort/hash
4. Number of blocks

### Attribute Type
Note that all attributes, X, Y and Z must be strings and Y may be a non-key attribute.

### Block Size
Assumed that each block can store 100 tuples for both relations, R and S.

### Input format
The bash script named `run.sh` is used to compile and run your code. The command for execution would be of the form:
`run.sh <path of R file> <path of S file><sort/hash> <M>`

### Output file
<R filename>_<S filename>_join.txt (Kindlynote it should contains only R & S base filename and not their path).
 
Use the generator folder to generate sample inputs.
