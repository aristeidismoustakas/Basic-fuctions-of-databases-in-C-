#include "mergejoin.h"
#include "mergesort.h"

using namespace std;
unsigned int numofpairs_mj=0;
unsigned int numofios_mj=0;
mergejoin::mergejoin()
{
    //ctor
}

mergejoin::~mergejoin()
{
    //dtor
}

int mergejoin::ElcompareNum ( record_t recA, record_t  recB)
{
    return (recA.num - recB.num);
}


int mergejoin::ElcompareId(record_t recA, record_t  recB)
{
    return (recA.recid - recB.recid);
}


int mergejoin::ElcompareString(record_t recA, record_t  recB)
{
    return (strcmp(recA.str,recB.str));
}


int mergejoin::ElcompareBoth(record_t recA, record_t  recB)
{
    if(!strcmp(recA.str,recB.str))
    {
        return (recA.num - recB.num);
    }else
    {
        return (strcmp(recA.str,recB.str));
    }
}

int mergejoin::compareRecords(record_t first,record_t second,char field)
{
    switch (field)
    {
        case '0':
            return ElcompareId(first,second);
            break;
        case '1':
            return ElcompareNum(first,second);
            break;
        case '2':
            return ElcompareString(first,second);
            break;
        case '3':
            return ElcompareBoth(first,second);
            break;
    }
    return 0;
}
//Fortwnei ena block se sygkekrimeni thesi tou buffer
bool mergejoin::loadBlockToBuffer(int numBlocksInFile,int nmem_blocks,int &counter,FILE *rFile,block_t *bigBuffer,int place)
{
    if(counter>=numBlocksInFile) return false;
    fseek(rFile,sizeof(block_t)*(counter),SEEK_SET);
    fread(&bigBuffer[place],sizeof(block_t),1,rFile);
    counter=counter+1;
    numofios_mj++;
    return true;
}


bool mergejoin::loadBlockToBufferOrFile(int numBlocksInSecondFile,int nmem_blocks,int &counterFile2,FILE *rFile2,FILE *dTempFile,block_t *bigBuffer,int &position)
{
    if(nmem_blocks>3)
    {
        return loadBlockToBuffer(numBlocksInSecondFile,nmem_blocks,counterFile2,rFile2,bigBuffer,2);
    }
    else
    {
        fwrite(&bigBuffer[1],sizeof(block_t),1,dTempFile);
        return loadBlockToBuffer(numBlocksInSecondFile,nmem_blocks,counterFile2,rFile2,bigBuffer,1);
    }
}
void mergejoin::writeOutput(block_t* bigBuffer,int nmem_blocks,int &numOfOutPut,int &counterOutput,FILE* dFile)
{
    bigBuffer[nmem_blocks-1].nreserved=numOfOutPut;
    bigBuffer[nmem_blocks-1].blockid=counterOutput++;
    fwrite(&bigBuffer[nmem_blocks-1],sizeof(block_t),1,dFile);
    numofpairs_mj+=numOfOutPut;
    numOfOutPut = 0;
    numofios_mj++;
}

bool mergejoin::reverseFilePointers(FILE* rFile1,FILE* rFile2,int &counterFile1,int counterFile2,int numBlocksInFirstFile,int numBlocksInSecondFile,int nmem_blocks,block_t* bigBuffer,char field)
{
    if( compareRecords(bigBuffer[0].entries[0],bigBuffer[1].entries[0],field) > 0 )
    {
        fclose(rFile1);
        fclose(rFile2);
        counterFile1 = 0;
        counterFile2 = 0;
        rFile1=fopen("sortedfile2.bin","rb");
        rFile2=fopen("sortedfile1.bin","rb");
        loadBlockToBuffer(numBlocksInFirstFile,nmem_blocks,counterFile1,rFile1,bigBuffer,0);
        loadBlockToBuffer(numBlocksInSecondFile,nmem_blocks,counterFile2,rFile2,bigBuffer,1);
        return true;
    }
    return false;
}
void mergejoin::saveRecordToOutput(bool isReversed,record_t firstRecord,record_t secondRecord,block_t* bigBuffer,int nmem_blocks,int &numOfOutPut)
{
    record_t out;
    if(!isReversed)
    {
        out = firstRecord;
        out.rdummy1 = secondRecord.recid;
    }
    else
    {
        out = secondRecord;
        out.rdummy1 = firstRecord.recid;
    }
    bigBuffer[nmem_blocks-1].entries[numOfOutPut++] = out;
}

void mergejoin::skipBlocks(int &counterFile2,int &idiaBlock,bool &firstLoad)
{
    if(counterFile2-idiaBlock >0)
    {
        counterFile2 = counterFile2 - idiaBlock;
    }
    if(idiaBlock>1)
    {
        firstLoad = true;
        idiaBlock = 0;
    }
}

int mergejoin::do_mergejoin(char* file1,char* file2,char* output,int numBlocksInFirstFile,int numBlocksInSecondFile,int nmem_blocks,char field,unsigned int *nres,unsigned int *nios)
{
    numofpairs_mj=0;
    numofios_mj=0;
    block_t *bigBuffer=new block_t[nmem_blocks];
    mergesort myMergeSort;
    mergesort myMergeSort1;
    unsigned int* npasses3;
    npasses3=new unsigned int[1];
    unsigned int* nios3;
    nios3 = new unsigned int[1];
    unsigned int* nsorted_segs3;
    nsorted_segs3 = new unsigned int[1];
    unsigned int* npasses4;
    npasses4=new unsigned int[1];
    unsigned int* nios4;
    nios4 = new unsigned int[1];
    unsigned int* nsorted_segs4;
    nsorted_segs4 = new unsigned int[1];
    MergeSort(file2,field,bigBuffer,nmem_blocks,"sortedfile2.bin",nsorted_segs4,npasses4,nios4);
    MergeSort(file1,field,bigBuffer,nmem_blocks,"sortedfile1.bin",nsorted_segs3,npasses3,nios3);
    numofios_mj += *nios4;
    numofios_mj += *nios3;
    FILE *rFile1=fopen("sortedfile1.bin","rb");
    FILE *rFile2=fopen("sortedfile2.bin","rb");
    FILE *dTempFile=fopen("temp1.bin","wb");
    FILE *dFile=fopen(output,"wb");
    int counterFile1 = 0;
    int counterFile2 = 0;
    int counterOutput = 0;
    int numOfOutPut = 0;
    bool isEqual = true;
    int k=0;
    int posInBuf=0;
    int position = 1;
    bool isFinished=false;
    bool firstLoad = true;
    bool lastBlock = false;
    bool isReversed = false;
    int idiaBlock = 0;
    record_t firstRecord,secondRecord,nextRecord;

    loadBlockToBuffer(numBlocksInFirstFile,nmem_blocks,counterFile1,rFile1,bigBuffer,0);
    loadBlockToBuffer(numBlocksInSecondFile,nmem_blocks,counterFile2,rFile2,bigBuffer,1);
    isReversed = reverseFilePointers(rFile1,rFile2,counterFile1,counterFile2,numBlocksInFirstFile,numBlocksInSecondFile,nmem_blocks,bigBuffer,field);
    while(!isFinished)
    {
        //Loop through the records of 1st file
        for(int j=0;j<bigBuffer[0].nreserved;j++)
        {
            firstRecord = bigBuffer[0].entries[j];
            if(j<bigBuffer[0].nreserved - 1)
            {
                nextRecord = bigBuffer[0].entries[j+1];
            }else
            {
                if(!loadBlockToBuffer(numBlocksInFirstFile,nmem_blocks,counterFile1,rFile1,bigBuffer,0))
                {
                    isFinished=true;
                    nextRecord = bigBuffer[0].entries[j];
                }
                nextRecord = bigBuffer[0].entries[0];
            }
            isEqual = true;
            //Me to pou vroume 1 pou den einai idio prepei na vgoume apo ti 2i for
            //Ean teleiwsei i for kai exoume akoma idia prepei na fortwsoume to epomeno block apo to 2o arxeio;
            k=posInBuf;
            while(isEqual)
            {
                if(lastBlock && compareRecords(firstRecord,nextRecord,field)==0)
                {
                    position = 2;
                }
                secondRecord = bigBuffer[position].entries[k++];

                while(compareRecords(firstRecord,secondRecord,field)>0)
                {
                    secondRecord = bigBuffer[position].entries[k++];
                    if(k>=bigBuffer[position].nreserved)
                    {
                        if(loadBlockToBufferOrFile(numBlocksInSecondFile,nmem_blocks,counterFile2,rFile2,dTempFile,bigBuffer,position))
                        {
                            position = 2;
                            k=0;
                        }
                        else
                        {
                            break;
                        }
                    }
                }

                if(counterFile2 > numBlocksInSecondFile)
                {
                    lastBlock = true;
                    position = 2;
                }


                if((compareRecords(firstRecord,secondRecord,field))==0)
                {
                    if(numOfOutPut>=MAX_RECORDS_PER_BLOCK)
                    {
                        writeOutput(bigBuffer,nmem_blocks,numOfOutPut,counterOutput,dFile);
                    }
                    saveRecordToOutput(isReversed,firstRecord,secondRecord,bigBuffer,nmem_blocks,numOfOutPut);
                    if(k==bigBuffer[position].nreserved)
                    {
                        if(firstLoad)
                        {
                            if(!loadBlockToBufferOrFile(numBlocksInSecondFile,nmem_blocks,counterFile2,rFile2,dTempFile,bigBuffer,position))
                            {
                                lastBlock = true;
                                isEqual = false;
                            }
                            else
                            {
                                firstLoad = false;
                                position = 2;
                            }
                        }
                        else
                        {
                            if(position == 2)
                            {
                                if(counterFile2 < numBlocksInSecondFile)
                                {
                                    posInBuf=0;
                                    loadBlockToBufferOrFile(numBlocksInSecondFile,nmem_blocks,counterFile2,rFile2,dTempFile,bigBuffer,position);
                                    idiaBlock++;
                                }
                                else
                                {
                                    if(!lastBlock)
                                    {
                                        isEqual = false;
                                        posInBuf = 0;
                                        position = 1;
                                        skipBlocks(counterFile2,idiaBlock,firstLoad);
                                    }
                                    else
                                    {
                                        position = 2;
                                        isEqual = false;
                                        isFinished = true;
                                    }
                                }
                            }
                            else
                            {
                                position = 2;
                            }
                        }
                        if(!lastBlock)   k=0;
                        else k = k%bigBuffer[position].nreserved;
                    }
                }
                else
                {
                    isEqual = false;
                    if((compareRecords(firstRecord,nextRecord,field)!=0))
                    {
                        if(position==2)
                        {
                            if(counterFile2<=numBlocksInSecondFile)
                            {
                                bigBuffer[1]=bigBuffer[2];
                                idiaBlock = 0;
                                loadBlockToBufferOrFile(numBlocksInSecondFile,nmem_blocks,counterFile2,rFile2,dTempFile,bigBuffer,position);
                                if(counterFile2 > numBlocksInSecondFile)
                                {
                                    lastBlock = true;
                                    position = 2;
                                }
                            }
                            else
                            {
                                bigBuffer[1]=bigBuffer[2];
                                idiaBlock=0;
                                if(counterFile1>=numBlocksInFirstFile)
                                {
                                    isFinished=true;
                                }
                            }
                        }
                        if(k>0)
                        {
                            posInBuf=k-1;
                        }
                        else
                        {
                            posInBuf = k;
                        }
                    }
                    else
                    {
                        skipBlocks(counterFile2,idiaBlock,firstLoad);
                        k=0;
                    }
                    position = 1 ;
                }

                if(numBlocksInSecondFile<numOfOutPut && k>=MAX_RECORDS_PER_BLOCK)
                {
                    isEqual = false;
                }

            }
        }
    }

    if(numOfOutPut>0)
    {
        writeOutput(bigBuffer,nmem_blocks,numOfOutPut,counterOutput,dFile);
    }


    fclose(dFile);
    fclose(rFile1);
    fclose(rFile2);
    fclose(dTempFile);
    *nres=numofpairs_mj;
    *nios=numofios_mj;
    remove("sortedfile1.bin");
    remove("sortedfile2.bin");
    remove("temp1.bin");
}
