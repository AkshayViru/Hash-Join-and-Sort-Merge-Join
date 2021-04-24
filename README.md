# Hash-Join-and-Sort-Merge-Join
Implementation of database relations join operators - Sort Merge Join and Hash Join

# Implementation
Given M memory blocks and two large relations R(X,Y)and S(Y,Z). There are iterators for the following operations.
- SortMerge Join○
  - open()- Create sorted sublists for R and S, each of size M blocks.
  - getnext()- Use 1 block for each sublist and get minimum of R & S. Join this minimum Y value with the other table and return. Checks for B(R)+B(S)<M^2
  - close()- close all files
- Hash Join
  - open()- Create M1 hashed sublists for R and S
  - getnext()- For each Ri and Si thus created, load the smaller of the two in the main memory and create a search structure over it.You can use M1 blocks to achieve this. Then recursively load the other file in the remaining blocks and for each record of this file, search corresponding records (with same join attribute value) from the other file. Checks for min(B(R),B(S))<M^2
  - close()- close all files
