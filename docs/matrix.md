---
title: DS Phase 0 (Team 31)
author:
    - P Shiridi Kumar (2021121005)
    - Shikhar Saxena (2021121010)
---

# Part 2

## **Page layout**

- Given Constraints and assumptions : 
    -   Row length can be less than ,equal or greater than the block size
    - Since the layout is regarding matrix rowCount is equal to column Count
    - All the rows are of fixed size
- The matrix page layout follows a spanned allocation where the sub sequent rows breaks and are spanned along different blocks when a block is full.
- Eg : Given a matrix of dimensions 1001x1001 and block size of 1000 integers (4k) the first three rows completely fits in to the block where as the fourth block doesnt completely fit into th block (First 997 will be stored into the first block and the next 4 elements will be stored in the sub sequent blocks)
- Since the rows are of fixed size there isn't  any explicit need of seperators(although some internal seperator variables are used in the project)
- pageIndex of a starting element of a row can be caluculated using :
    >  (rowIndex*ColumnCount)/Max_no_of_Elements_in_a_Block
- As of now the load matrix command goes into the same function calls as like load table but are internally seperated using some conditional statements
- Code Flow(rough overview : implemented in blockify() in table.cpp 182 else statement) 

```c
   while (getline(fin, line))
        {
            stringstream s(line);
            for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
            {
                if (!getline(s, word, ','))
                    return false;
                row[ind] = stoi(word);
                rowsInPage[0][ind] = row[ind];
                ind++;
                if (ind == maxElementsperblock)
                {
                    bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter,sep,this->start);
                    this->start=sep.size();
                    ind = 0;
                    pageCounter = 0;
                    this->blockCount++;
                }
            }
            this->sep.push_back(ind);

            sep.push_back(ind);
            pageCounter++;
            this->updateStatistics(row);
        }
    }
```

- Note : we have added additional functions in various header files classes(like page.h and etc and some of them overloaded functions with same function name and different parametersetc)

***

## **Cross Transpose**

- cross transpose function has been implemented in cross_transpose.cpp file which redirects to transposeLine function in Cursor.cpp
- There will be two different tarversals for both the matrices .row wise for one matrice and Column wise for the other matrix
- In each iteration the row wise traversal matrix fetches the corresponding row(**the point until it fits in the block**) and the other matrix fetches the column(**the point until the it fits in the block**)
- Whenever the row is spanned across multiple blocks the cursor is shifted correspondingly and the remaining elements are fetched.
- After fetching the row and column values upto the point till which it will fit in the block , the Corresponding rows variables in page(refer to page.h) is updated accordingly with each other .
- So in row wise traversal since the traversal is sequential the elements are consecutively fetched from one block and whenever it reaches the block end or if the Cursor is changed to the next page the rows variable is written into corresponding page block file.
- Approximately same approach has been followed for columnwise traversal matrix .
- In order to export the matrix and see the transposed matrix in original file ,export command can be used.
- Majorly responsible function calls:
     >void Cursor::transposeLine(int row)

     >vector<int> Cursor::getnextline(int j, int start, int end, int pageindex, vector<int> &res,int write)

     and some othe utility functions in page.cpp and etc.
- There is no additional memory is used except for two constant length vectors which are equal to page size named as res and res1.
- These vectors are cleared periodically whenever the current page block is full and guarantee to not exceed the page size.

# Part 3 Discussion

We assume that the user specifies that the matrix is `SPARSE` while giving input.

## Compression Technique

We store each row of the sparse matrix as a linked list. Here, each node of the linked list contains the column index (from the original matrix) and the associated matrix value.

So, for example if
```
        1 3 0 0 0 
A  = [  0 2 5 0 0 ]
        1 0 0 0 3
        0 0 -2 0 0
        0 0 0 0 0
```

then the linked list will be like this:

```
Node: (column_index, value)

Linked List:

row1: (0, 1) -> (1, 3)
row2: (1, 2) -> (2, 5)
row3: (0, 1) -> (4, 3)
row4: (2, -2)
row5: 
```

Since, we only store the non-zero values, this approach is fairly efficient for sparse matrices. And we can use this to save memory and space in our system.

## Associated Page layout

Since our approach consists of variable-length rows, we will have a row separator to be stored in the pages as well; that will mark the ending of each row.

For a particular row, let the number of nodes be *n*.

Then for this row, *2n* values will be stored (the column index and then the node value). 

Rest of the page layout remains same; in that each row will be stored in the pages (separated by the row separator). If one page is completely written to, then we will store the rest of the row contents in a new page (in a similar way to our original approach). Here, the row separator will be present in the new block so that we keep reading into the next block for this row (and not stop in the first block). Here we will have another block separator index that will note the offset which denotes the index from where the row has been cut (between the two pages).

## Transpose of the sparse matrix

For transposing the sparse matrix, we can just find the row index by counting the row separators. Using the row separators, we can also get the starting of each row.

For transposing, we'll create a map of linked lists for implementing the above mentioned page layout in the main memory (which doesn't exceed the page block size for the given page). Each key in the map will be the new row index and the linked list will contain the transposed column index and associated value.

For creating this linked list, we traverse through our page block row-wise and for each column index and value (and associated row index) we can add this to the appropriate key (here, column index) into our map. The new node will be added as (row_index, value) in the linked list.

Whenever we reach the page block end (in the loaded matrix), we replace our map values into the original page block and clear the map (for further pages).

In order to export, traverse normally through the pages and print zeros until they see the column index due to which we will write the associated node value.