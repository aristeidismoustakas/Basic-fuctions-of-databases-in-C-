#ifndef HASHJOIN_H
#define HASHJOIN_H
#include <iostream>
#include<fstream>
#include "dbtproj.h"
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */
#include <string.h>
#include <algorithm>
#include <math.h>
#include "hashjoin.h"

class hashjoin
{
    public:
        hashjoin();
        virtual ~hashjoin();
        int ElcompareNum ( record_t recA, record_t  recB);
        int ElcompareId(record_t recA, record_t  recB);
        int ElcompareString(record_t recA, record_t  recB);
        int ElcompareBoth(record_t recA, record_t  recB);
        int compareRecords(record_t first,record_t second,char field);
        bool createTempFiles(FILE *allFiles[],int nmem_blocks);
        bool changePointers(FILE *allFiles[],int nmem_blocks);
        bool deleteTempFiles(FILE *allFiles[],int nmem_blocks);
        int hashFunction(record_t rec,int num,int nmem_blocks,char field);
        bool putInTheBins(record_t rec,int hashCode,block_t *bigBuffer,FILE *allFiles[],int nextPos[],int numOfFiles[]);
        int dohashjoin(char* file1,char* file2,char* output,int numBlocksInFirstFile,int numBlocksInSecondFile,int nmem_blocks,char field,block_t *bigBuffer,unsigned int *,unsigned int *);
        bool probing(FILE *myFile,FILE *outPut,block_t *bigBuffer,int num,FILE *allFiles[],int nextPos[],int numOfFiles[],int nmem_blocks,char field);
        int checkTheBin(FILE *outFile,record_t rec,int hashCode,block_t *bigBuffer,int nmem_blocks,FILE *allFiles[],int &outPutPointer,int &nextId,int numOfFiles[],char field);
        bool createTheHashTalble(FILE *myFile,block_t *bigBuffer,int num,FILE *allFiles[],int nextPos[],int numOfFiles[],int nmem_blocks,char field);
    protected:

    private:
};

#endif // HASHJOIN_H
