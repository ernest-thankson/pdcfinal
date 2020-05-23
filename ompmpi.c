#include <stdio.h>
#include <string.h>
#include<math.h>
#include<omp.h>

//code to do binary search retrieved from https://www.comrevo.com/2015/05/openmp-program-for-n-ary-search-algorithm.html

void printArray(int A[], int size);
void merge(int * X, int n, int * tmp);
void mergesort(int * X, int n, int * tmp);

int main(int argc, char* argv[]) {
    int* tmp;
    //number of rows in each file
    int numRows = 5;
    //Declaring arrays for tables
    int a1[numRows], a2[numRows], b[numRows], c[numRows], t[2], index;
    int aa1[2*numRows], aa2[2*numRows], bb[2*numRows], cc[2*numRows];
    FILE *fp = fopen("r1.csv", "r");

    if (!fp) {
        printf("Can't open file\n");
        return 0;
    }

    char buf[1024];
    int row_count = 0;
    int field_count = 0;
    char *ptr;
    int ret;
    int curInd = 0;
    //Somehow every even index of a1 code below ends up being filled with an unexpected number. 
    //The rest of the program has therefore been adjusted to work with this
    while (fgets(buf, 1024, fp)) {
        field_count = 0;
        row_count++;

        
        index = 0;
        char *field = strtok(buf, ",");
        while (field) {
            ret = strtol(field, &ptr, 10);
            printf("%d %d\n", ret, index);
            if (ret == 0 || ret == NULL)
            {
                ;
            }else if(index == 0){
               aa1[curInd] = ret;
            }else{
                bb[curInd] = ret;
            }
            field = strtok(NULL, ",");

            index++;
            field_count++;
            curInd++;
        }
        printf("\n");
    }

    printArray(aa1, 2*sizeof(a1)/sizeof(a1[0]));
    printArray(bb, 2*sizeof(b)/sizeof(b[0]));
    fclose(fp);


    fp = fopen("r2.csv", "r");
    field_count = 0;
    curInd = 0;
    //Somehow every even index of a1 code below ends up being filled with an unexpected number. 
    //The rest of the program has therefore been adjusted to work with this
    while (fgets(buf, 1024, fp)) {
        field_count = 0;
        row_count++;

        
        index = 0;
        char *field = strtok(buf, ",");
        while (field) {
            ret = strtol(field, &ptr, 10);
            printf("%d %d\n", ret, index);
            if (ret == 0 || ret == NULL)
            {
                ;
            }else if(index == 0){
               aa2[curInd] = ret;
            }else{
                cc[curInd] = ret;
            }
            field = strtok(NULL, ",");

            index++;
            field_count++;
            curInd++;
        }
        printf("\n");
    }

    printArray(a2, 2*sizeof(a1)/sizeof(a1[0]));
    

    printf("%s\n","checking..." );
    printArray(a2, 2*sizeof(a1)/sizeof(a1[0]));

    fclose(fp);



    int my_rank, p; // process rank and number of processes
    int source, dest; // rank of sender and receiving process
    int tag = 0; // tag for messages
    MPI_Status status; // stores status for MPI_Recv statements
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &p);
    if (my_rank == 0)
    {
        dest = 1; // sets destination for MPI_Send to process 0
        MPI_Send(a1, numRows, MPI_INT, dest,tag, MPI_COMM_WORLD);
        MPI_Send(b, numRows, MPI_INT, dest,tag, MPI_COMM_WORLD);
        dest = 2;
        MPI_Send(a2, numRows, MPI_INT, dest,tag, MPI_COMM_WORLD);
        MPI_Send(c, numRows, MPI_INT, dest,tag, MPI_COMM_WORLD);
    } // sends string to process 0
    else if(my_rank == 1) { 
        MPI_Recv(a1, numRows, MPI_INT, 0, tag, MPI_COMM_WORLD, &status);
        MPI_Recv(b, numRows, MPI_INT, 0, tag, MPI_COMM_WORLD, &status);
        int bitString[numRows];
        //Initializing all positions of bit strings to 0
        for (int i = 0; i < numRows; ++i)
        {
            
            bitString[i] = 0;
        }
        int val1, val2, val3; //to store hash values of the three hash functions temporarily
        //Applying hash functions to all keys in a1
        for (int i = 0; i < numRows; ++i)
        {
            val1 = h1(a1[i], numRows);
            val2 = h2(a1[i]);
            val3 = h3(a1[i]);
            bitString[val1] = 1;
            bitString[val2] = 1;
            bitString[val3] = 1;
        }
        //Sending bit string to node 2
        MPI_Send(a1, numRows, MPI_INT, 2, tag, MPI_COMM_WORLD);   
        
        //receiving tuples that may qualify for join
        MPI_Recv(newa2, numRows, MPI_INT, 2, tag, MPI_COMM_WORLD, &status);
        MPI_Recv(newc, numRows, MPI_INT, 2, tag, MPI_COMM_WORLD, &status);


        //declaring arrays to hold tuples for join table
        int finala[], finalb[], finalc[];

        /*
        for each key in a1, a binary search is made in newa2. A short linear search uses the position returned 
        by th binary search as a starting position to locate all tuples that match. All repetition of the key in a1 are
        also located before the join is made
        */
        int pos, isRepeat, a1Start, a1End, a2Start, a2End;

        int index = 0;
        
        for (int i = 0; i < numRows; ++i)
        {
            if (a1[i] == a[i-1] && i > 0 )
            {
                ;
            } else if(i == numRows-1){
                ;
            }else{
                a1Start = a1End = i;
            
                pos = binarySearch(newa2, a1[i], numRows); //REMEMBER TO CHECK WHETHER ITEM WAS NOT FOUND FIRST |||||||
                a2Start=a2End=pos;

                //increasing a1End to last occurrence of current key
                while (a1[a1Start+1] == a1[i]){
                    a2End++;
                }

                //decreasing a2start to position of first occurrence of current key
                while (a2[a2Start-1]==a2[pos]){
                    a2Start--;
                }
                //increasing a2start to position of last occurrence of current key
                while (a2[a2End+1]==a2[pos]){
                    a2End++;
                }

                //now that we have starting and ending positions for all tuples that share the same key a1[i] we can begin the join

                 for (int m = a1Start; m < a1End; ++m)
                 {
                    for (int n = a2Start; n < a2End; ++n)
                    {
                        finala[index] = a1[i];
                        finalb[index] = b[m];
                        finalc[index] = newc[n];
                        index++;
                    }
                 }

                //New table can now be written to file

                
            }
               
        }
    } else if (my_rank == 2) {
        //receiving arrays containing tuples for table r3
        MPI_Recv(a2, numRows, MPI_INT, 0, tag, MPI_COMM_WORLD, &status);
        MPI_Recv(c, numRows, MPI_INT, 0, tag, MPI_COMM_WORLD, &status);
        //sorting a2 and c based on a2

        //receiving bit string
        MPI_Recv(bitString, numRows, MPI_INT, 1, tag, MPI_COMM_WORLD, &status);

        //declaring arrays to contain tuples that may qualify for a join
        int newa2[numRows], newc[numRows];

        int val1, val2, val3; //to store hash values of the three hash functions temporarily
        int index = 0; //next position in newa2 and newc to be filled
        //hashing values of a2 and checking corresponding positions to see whether they contain the number 1
        for (int i = 0; i < numRows; ++i)
        {
            val1 = h1(a2[i]);
            val2 = h2(a2[i]);
            val3 = h3(a2[i]);
            if (bitString[val1] == 1 || bitString[val2] == 1 || bitString[val3] == 1)
            {
                newa2[index] = a2[i];
                newc[index] = c[i];
                index++;
            }
        }

        //Sending tuples that may qualify for join back to node 1
        MPI_Send(newa2, numRows, MPI_INT, 1,tag, MPI_COMM_WORLD);
        MPI_Send(newc, numRows, MPI_INT, 1,tag, MPI_COMM_WORLD);


    }

     MPI_Finalize(); // shuts down MPI


    return 0;
}

// void printArray(int A[], int size) 
// { 
//     int i; 
//     for (i=0; i < size; i++) 
//         if (i%2==0)
//         {
//             printf("%d ", A[i]);
//         }
         
//     printf("\n"); 
// } 
void printArray(int A[], int size) 
{ 
    int i; 
    for (i=0; i < size; i++) 
        printf("%d ", A[i]);         
    printf("\n"); 
} 

int h1(int element, int table_size){ //Division method
    return element%table_size;
}

int h2(int key, int len){ //Mid Square method
    return element%table_size;
}

int h3(int key, int len){ //Digit Folding method
    return element%table_size;
}

//Mergsort function

void merge(int * X, int n, int * tmp) {
   int i = 0;
   int j = n/2;
   int ti = 0;

   while (i<n/2 && j<n) {
      if (X[i] < X[j]) {
         tmp[ti] = X[i];
         ti++; i++;
      } else {
         tmp[ti] = X[j];
         ti++; j++;
      }
   }
   while (i<n/2) { /* finish up lower half */
      tmp[ti] = X[i];
      ti++; i++;
   }
      while (j<n) { /* finish up upper half */
         tmp[ti] = X[j];
         ti++; j++;
   }
   memcpy(X, tmp, n*sizeof(int));

} // end of merge()

void mergesort(int * X, int n, int * tmp)
{
   if (n < 2) return;

   #pragma omp task firstprivate (X, n, tmp)
   mergesort(X, n/2, tmp);

   #pragma omp task firstprivate (X, n, tmp)
   mergesort(X+(n/2), n-(n/2), tmp);

   #pragma omp taskwait

    /* merge sorted halves into sorted list */
   merge(X, n, tmp);
}

int binarySearch(int array[], int key, int size){
    int sep[20],i,j,n,left,right,interval,index,break_value=0,tid;
     
     n = 2;

     left=0;
     right=size-1;

     if(key>=array[left] && key<=array[right])
      {
       while(left!=right)
       {
         // (start) code to find seperators
         printf("left=%d, right=%d, size=%d\n",left,right,size);
         if(size<=n)
          {
           #pragma omp parallel for num_threads(size)
           for(i=0;i<size;i++)
            {
             sep[i]=left+i;
             tid=omp_get_thread_num();
             printf("Thread %d allocated sep[%d]=%d\n",tid,i,sep[i]);
            }
          }

         else
          {
           sep[0]=left;
           interval=ceil((float)size/(float)n);
         
           #pragma omp parallel for num_threads(n-1)
           for(i=1;i<=n-1;i++)
            {
             sep[i]=left+interval*i-1;
             tid=omp_get_thread_num();
             printf("Thread %d allocated sep[%d]=%d\n",tid,i,sep[i]);
            }

            sep[n]=right;
           }
          // (end) Code to find seperators

          // (start) Code for comparison

          for(i=0;i<=n;i++)
           {
             if(key==array[sep[i]])
              {
                index=sep[i];
                printf("Element found at position %d\n",index+1);
                break_value=1;
                break;
              }
             if(key<array[sep[i]])
              {
                right=sep[i];
                if(i!=0)
                  left=1+sep[i-1];
                size=right-left+1;
                break;
              }
           }

          // (end) Code for comparison

          if(break_value==1)
            break;
       } //End of 'while' loop
      } //End of 'if'

    if(left==right || !(key>=array[left] && key<=array[right]))
      printf("Element does not present in the list\n");
}