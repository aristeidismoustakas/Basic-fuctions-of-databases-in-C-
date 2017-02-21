#ifndef MERGESORT_H
#define MERGESORT_H
#include <iostream>
#include<fstream>
#include "dbtproj.h"
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */
#include <string.h>
#include <algorithm>
#include <math.h>
#include <string.h>
#include "mergesort.h"

class mergesort
{
public:
    mergesort();
    virtual ~mergesort();
    static int compareNum (const void * a, const void * b);
    static int compareId(const void * a, const void * b);
    static int compareString(const void * a, const void * b);
    static int compareBoth(const void * a, const void * b);
    int sortBlocks(block_t *buffer,int num,char field);
    bool isFinished(int  *pointers,block_t *buffer,int nmem_numblocks,int level,FILE *rFile,int *lastRead,int k,int notUsed,int partFill);
    bool merging(block_t *buffer,int nmem_numblocks,char field,int numBlocksInFile,FILE *pFile,int &counter,int level,FILE *rFile,int k,int notUsed,int partFill);
    bool do_mergesort(const char* input,const char* output,int numBlocksInFile,int nmem_blocks,char field,unsigned int *nsorted_segments,unsigned int *npasses,unsigned int *nios);
protected:

private:
};

#endif // MERGESORT_H
