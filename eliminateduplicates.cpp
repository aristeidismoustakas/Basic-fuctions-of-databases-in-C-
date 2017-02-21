#include "eliminateduplicates.h"
#include "mergesort.h"


using namespace std;

unsigned int uniques=0;
unsigned int ioss=0;

eliminateduplicates::eliminateduplicates()
{
    //ctor
}

eliminateduplicates::~eliminateduplicates()
{
    //dtor
}


int eliminateduplicates::loadbuffer(int numBlocksInFile,int nmem_blocks,int &counter,FILE *rFile,block_t *bigBuffer)
{
    if(counter+nmem_blocks-1<=numBlocksInFile)
    {
        fseek(rFile,sizeof(block_t)*(counter),SEEK_SET);
        fread(bigBuffer,sizeof(block_t),nmem_blocks-1,rFile);
        ioss++;
        counter=counter+nmem_blocks-1;
        if(counter==numBlocksInFile)
            return -1;
        return 0;

    }
    else
    {
        int notused=counter+nmem_blocks-1-numBlocksInFile;
        int upoloipa=nmem_blocks-1-notused;
        fseek(rFile,sizeof(block_t)*(counter),SEEK_SET);
        fread(bigBuffer,sizeof(block_t),upoloipa,rFile);
        ioss++;
        return notused;
    }

}

int eliminateduplicates::ElcompareNum ( record_t recA, record_t  recB)
{
    return (recA.num - recB.num);
}


int eliminateduplicates::ElcompareId(record_t recA, record_t  recB)
{
    return (recA.recid - recB.recid);
}


int eliminateduplicates::ElcompareString(record_t recA, record_t  recB)
{
    return (strcmp(recA.str,recB.str));
}


int eliminateduplicates::ElcompareBoth(record_t recA, record_t  recB)
{
    if(!strcmp(recA.str,recB.str))
    {
        return (recA.num - recB.num);
    }
    else
    {
        return (strcmp(recA.str,recB.str));
    }
}


int eliminateduplicates::doEliminateDuplicates(const char* input,const char* output,block_t *bigBuffer,int numBlocksInFile,int nmem_blocks,char field,unsigned int *nun,unsigned int *nios)
{
    uniques=0;
    ioss=0;
    int numOfOutPut=0;
    int counter=0;
    mergesort mySort;
    unsigned int* npasses2;
    npasses2 = new unsigned int[1];
    unsigned int* nios2;
    nios2 = new unsigned int[1];
    unsigned int* nsorted_segs2;
    nsorted_segs2 = new unsigned int[1];
    mySort.do_mergesort(input,output,numBlocksInFile,nmem_blocks,field,nsorted_segs2,npasses2,nios2);
    rename(output,"temp.bin");
    FILE *rFile=fopen("temp.bin","rb");
    FILE *dFile=fopen(output,"wb");
    record_t myrecord;
    int notused=0;
    int last=0;
    bool isFirst=true;
    while(notused==0)
    {
        notused=loadbuffer(numBlocksInFile,nmem_blocks,counter,rFile,bigBuffer);
        if(isFirst)
        {
            myrecord=bigBuffer[0].entries[0];
            isFirst=false;
        }
        int notusedBlocks=notused;
        if(notused==-1)
            notusedBlocks=0;
        for(int i=0; i<nmem_blocks-1-notusedBlocks; i++)
        {
            for(int j=0; j<MAX_RECORDS_PER_BLOCK; j++)
            {
                switch (field)
                {
                case '0':
                    if(!(ElcompareId(myrecord,bigBuffer[i].entries[j])==0))
                    {
                        bigBuffer[nmem_blocks-1].entries[last++]=myrecord;
                        myrecord=bigBuffer[i].entries[j];
                    }
                    break;
                case '1':
                    if(!(ElcompareNum(myrecord,bigBuffer[i].entries[j])==0))
                    {
                        bigBuffer[nmem_blocks-1].entries[last++]=myrecord;
                        myrecord=bigBuffer[i].entries[j];
                    }
                    break;
                case '2':
                    if(!(ElcompareString(myrecord,bigBuffer[i].entries[j])==0))
                    {
                        bigBuffer[nmem_blocks-1].entries[last++]=myrecord;
                        myrecord=bigBuffer[i].entries[j];
                    }
                    break;
                case '3':
                    if(!(ElcompareBoth(myrecord,bigBuffer[i].entries[j])==0))
                    {
                        bigBuffer[nmem_blocks-1].entries[last++]=myrecord;
                        myrecord=bigBuffer[i].entries[j];
                    }
                    break;
                }
                if(last==MAX_RECORDS_PER_BLOCK)
                {
                    bigBuffer[nmem_blocks-1].nreserved=last;
                    bigBuffer[nmem_blocks-1].blockid=numOfOutPut++;
                    fwrite(&bigBuffer[nmem_blocks-1],sizeof(block_t),1,dFile);
                    ioss++;
                    uniques+=8;
                    last=0;
                }
            }

        }
        if(notused!=0 && last!=0)
        {
            bigBuffer[nmem_blocks-1].entries[last++]=myrecord;
            bigBuffer[nmem_blocks-1].nreserved=last;
            bigBuffer[nmem_blocks-1].blockid=numOfOutPut++;
            fwrite(&bigBuffer[nmem_blocks-1],sizeof(block_t),1,dFile);//grafw to teleutaio
            ioss++;
            uniques+=last;
        }
    }
    fclose(dFile);
    fclose(rFile);
    remove("temp.bin");
    *nun=uniques;
    *nios=ioss+*nios2;  //add and the ios of the mergesort
}
