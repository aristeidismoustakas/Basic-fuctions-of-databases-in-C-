#ifndef MERGEJOIN_H
#define MERGEJOIN_H
#include <iostream>
#include<fstream>
#include "dbtproj.h"
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */
#include <string.h>
#include <algorithm>
#include <math.h>
#include "mergejoin.h"
#include "mergesort.h"

class mergejoin
{
public:
    mergejoin();
    virtual ~mergejoin();
    int ElcompareNum ( record_t recA, record_t  recB);
    int ElcompareId(record_t recA, record_t  recB);
    int ElcompareString(record_t recA, record_t  recB);
    int ElcompareBoth(record_t recA, record_t  recB);
    int compareRecords(record_t first,record_t second,char field);
    void writeOutput(block_t* bigBuffer,int nmem_blocks,int &numOfOutPut,int &counterOutput,FILE* dFile);
    bool reverseFilePointers(FILE* rFile1,FILE* rFile2,int &counterFile1,int counterFile2,int numBlocksInFirstFile,int numBlocksInSecondFile,int nmem_blocks,block_t* bigBuffer,char field);
    void saveRecordToOutput(bool isReversed,record_t firstRecord,record_t secondRecord,block_t* bigBuffer,int nmem_blocks,int &numOfOutPut);
    void skipBlocks(int &counterFile2,int &idiaBlock,bool &firstLoad);
    bool loadBlockToBuffer(int numBlocksInFile,int nmem_blocks,int &counter,FILE *rFile,block_t *bigBuffer,int place);
    bool loadBlockToBufferOrFile(int numBlocksInSecondFile,int nmem_blocks,int &counterFile2,FILE *rFile2,FILE *dTempFile,block_t *bigBuffer,int &position);
    int do_mergejoin(char* file1,char* file2,char* output,int numBlocksInFirstFile,int numBlocksInSecondFile,int nmem_blocks,char field,unsigned int *nres,unsigned int *nios);
protected:

private:
};

#endif // MERGEJOIN_H
