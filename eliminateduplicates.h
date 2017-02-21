#ifndef ELIMINATEDUPLICATES_H
#define ELIMINATEDUPLICATES_H
#include <iostream>
#include<fstream>
#include "dbtproj.h"
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */
#include <string.h>
#include <algorithm>
#include <math.h>
#include <string.h>

class eliminateduplicates
{
    public:
        eliminateduplicates();
        virtual ~eliminateduplicates();
        int loadbuffer(int numBlocksInFile,int nmem_blocks,int &counter,FILE *rFile,block_t *bigBuffer);
        int ElcompareNum ( record_t recA, record_t  recB);
        int ElcompareId(record_t recA, record_t  recB);
        int ElcompareString(record_t recA, record_t  recB);
        int ElcompareBoth(record_t recA, record_t  recB);
        int doEliminateDuplicates(const char* input,const char* output,block_t *bigBuffer,int numBlocksInFile,int nmem_blocks,char field,unsigned int *,unsigned int *);

    protected:

    private:
};

#endif // ELIMINATEDUPLICATES_H
