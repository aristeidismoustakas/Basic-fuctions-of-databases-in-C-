#include <iostream>
#include<fstream>
#include "dbtproj.h"
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */
#include <string.h>
#include <algorithm>
#include <math.h>
#include <string.h>
#include "hashjoin.h"
#include "eliminateduplicates.h"
#include "mergejoin.h"
#include "mergesort.h"

using namespace std;
void MergeSort (char *infile, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nsorted_segs, unsigned int *npasses, unsigned int *nios)
{
    FILE *rFile1=fopen(infile,"rb");
    fseek(rFile1, 0L, SEEK_END);
    long mysize=ftell (rFile1);
    int numBlocksInFile = mysize / sizeof(block_t);
    fclose(rFile1);
    mergesort myMergeSort;
    myMergeSort.do_mergesort(infile,outfile,numBlocksInFile,nmem_blocks,field,nsorted_segs,npasses,nios);
}



void EliminateDuplicates (char *infile, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nunique, unsigned int *nios)
{
    FILE *rFile1=fopen(infile,"rb");
    fseek(rFile1, 0L, SEEK_END);
    long mysize=ftell (rFile1);
    int numOfFirstFile = mysize / sizeof(block_t);
    fclose(rFile1);
    eliminateduplicates myEliminateDupl;
    myEliminateDupl.doEliminateDuplicates(infile,outfile,buffer,numOfFirstFile,nmem_blocks,field,nunique,nios);
}



void MergeJoin (char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nres, unsigned int *nios)
{
    FILE *rFile1=fopen(infile1,"rb");
    fseek(rFile1, 0L, SEEK_END);
    long mysize=ftell (rFile1);
    int numOfFirstFile = mysize / sizeof(block_t);
    fclose(rFile1);
    FILE *rFile2=fopen(infile2,"rb");
    fseek(rFile2, 0L, SEEK_END);
    long mysize2=ftell (rFile2);
    int numOfSecondFile = mysize2 / sizeof(block_t);
    fclose(rFile2);
    mergejoin myMergeJoin;
    myMergeJoin.do_mergejoin(infile1,infile2,outfile,numOfFirstFile,numOfSecondFile,nmem_blocks,field,nres,nios);
}



void HashJoin (char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nres, unsigned int *nios)
{
    FILE *rFile1=fopen(infile1,"rb");
    fseek(rFile1, 0L, SEEK_END);
    long mysize=ftell (rFile1);
    int numOfFirstFile = mysize / sizeof(block_t);
    fclose(rFile1);
    FILE *rFile2=fopen(infile2,"rb");
    fseek(rFile2, 0L, SEEK_END);
    long mysize2=ftell (rFile2);
    int numOfSecondFile = mysize2 / sizeof(block_t);
    fclose(rFile2);
    hashjoin myHashJoin;
    myHashJoin.dohashjoin(infile1,infile2,outfile,numOfFirstFile,numOfSecondFile,nmem_blocks,field,buffer,nres,nios);
}
