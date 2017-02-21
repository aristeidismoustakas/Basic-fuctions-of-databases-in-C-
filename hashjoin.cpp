#include "hashjoin.h"



using namespace std;



unsigned int numofpairs=0;
unsigned int numofios=0;

hashjoin::hashjoin()
{
    //ctor
}

hashjoin::~hashjoin()
{
    //dtor
}

int hashjoin::ElcompareNum ( record_t recA, record_t  recB)
{
    return (recA.num - recB.num);
}


int hashjoin::ElcompareId(record_t recA, record_t  recB)
{
    return (recA.recid - recB.recid);
}


int hashjoin::ElcompareString(record_t recA, record_t  recB)
{
    return (strcmp(recA.str,recB.str));
}


int hashjoin::ElcompareBoth(record_t recA, record_t  recB)
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


int hashjoin::compareRecords(record_t first,record_t second,char field)
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


bool hashjoin::createTempFiles(FILE *allFiles[],int nmem_blocks)
{
    char nameOfTheFile[10];
    for(int i=1; i<=nmem_blocks-2; i++)
    {
        itoa(i, nameOfTheFile, 10);
        strcat(nameOfTheFile,".bin");
        allFiles[i-1]=fopen(nameOfTheFile,"wb");
    }
}

bool hashjoin::changePointers(FILE *allFiles[],int nmem_blocks)
{
    char nameOfTheFile[10];
    for(int i=1; i<=nmem_blocks-2; i++)
    {
        fclose(allFiles[i-1]);
        itoa(i, nameOfTheFile, 10);
        strcat(nameOfTheFile,".bin");
        allFiles[i-1]=fopen(nameOfTheFile,"rb");
    }

}
bool hashjoin::deleteTempFiles(FILE *allFiles[],int nmem_blocks)
{
    char nameOfTheFile[10];
    for(int i=1; i<=nmem_blocks-2; i++)
    {
        fclose(allFiles[i-1]);
        itoa(i, nameOfTheFile, 10);
        strcat(nameOfTheFile,".bin");
        remove(nameOfTheFile);
    }
}

int hashjoin::hashFunction(record_t rec,int num,int nmem_blocks,char field)
{
    int ret;
    int A=51;
    int B=83;
    int C=3083;
    int D=1759;
    int E=1007;
    if(field=='0' )
    {
        ret=((A*rec.recid+B)%E)%(nmem_blocks-2);
    }
    else if(field=='1')
    {
        ret=((A*rec.num+B)%E)%(nmem_blocks-2);
    }
    else if(field=='2')
    {
        ret=((((A*rec.str[0])^3+(B*rec.str[1])^3)+rec.str[2])%E)%(nmem_blocks-2);
    }
    else if(field=='3')
    {
        ret=(A*rec.num+B+(((A*rec.str[0])^3+(B*rec.str[1])^3)+rec.str[2])%E)%(nmem_blocks-2);
    }
    return ret;

}

bool hashjoin::putInTheBins(record_t rec,int hashCode,block_t *bigBuffer,FILE *allFiles[],int nextPos[],int numOfFiles[])
{
    if(nextPos[hashCode]>=MAX_RECORDS_PER_BLOCK)
    {
        bigBuffer[hashCode].nreserved=MAX_RECORDS_PER_BLOCK;
        bigBuffer[hashCode].blockid=++(numOfFiles[hashCode]);
        fwrite(&bigBuffer[hashCode],sizeof(block_t),1,allFiles[hashCode]);
        numofios++;
        nextPos[hashCode]=0;
    }
    bigBuffer[hashCode].entries[nextPos[hashCode]]=rec;
    nextPos[hashCode]++;
}

bool hashjoin::createTheHashTalble(FILE *myFile,block_t *bigBuffer,int num,FILE *allFiles[],int nextPos[],int numOfFiles[],int nmem_blocks,char field)
{
    for(int i=0; i<num; i++)
    {
        fseek(myFile,sizeof(block_t)*(i),SEEK_SET);
        fread(&bigBuffer[nmem_blocks-2],sizeof(block_t),1,myFile);
        numofios++;
        for(int j=0; j<bigBuffer[nmem_blocks-2].nreserved; j++)
        {
            int hashCode=hashFunction(bigBuffer[nmem_blocks-2].entries[j],num,nmem_blocks,field);
            putInTheBins(bigBuffer[nmem_blocks-2].entries[j],hashCode,bigBuffer,allFiles,nextPos,numOfFiles);
        }
    }
    for(int j=0; j<nmem_blocks-2; j++)
    {
        if(nextPos[j]>0)
        {
            bigBuffer[j].nreserved=nextPos[j];
            bigBuffer[j].blockid=++(numOfFiles[j]);
            fwrite(&bigBuffer[j],sizeof(block_t),1,allFiles[j]);
            numofios++;
            nextPos[j]=0;
        }
    }
}

int hashjoin::checkTheBin(FILE *outFile,record_t rec,int hashCode,block_t *bigBuffer,int nmem_blocks,FILE *allFiles[],int &outPutPointer,int &nextId,int numOfFiles[],char field)
{
    for(int i=0; i<numOfFiles[hashCode]; i++)
    {
        fseek(allFiles[hashCode],sizeof(block_t)*(i),SEEK_SET);
        fread(&bigBuffer[nmem_blocks-2],sizeof(block_t),1,allFiles[hashCode]);
        numofios++;
        for(int j=0; j<bigBuffer[nmem_blocks-2].nreserved; j++)
        {
            if(compareRecords(bigBuffer[nmem_blocks-2].entries[j],rec,field)==0)
            {
                record_t out = bigBuffer[nmem_blocks-2].entries[j];
                out.rdummy1 = rec.recid;
                out.rdummy2 = rec.num;
                bigBuffer[nmem_blocks-1].entries[outPutPointer++] = out;
            }
            if(outPutPointer>=MAX_RECORDS_PER_BLOCK)
            {
                bigBuffer[nmem_blocks-1].nreserved=MAX_RECORDS_PER_BLOCK;
                bigBuffer[nmem_blocks-1].blockid=nextId++;
                //cout<<"nextId  "<<nextId<<endl;
                fwrite(&bigBuffer[nmem_blocks-1],sizeof(block_t),1,outFile);
                numofpairs+=MAX_RECORDS_PER_BLOCK;
                numofios++;
                outPutPointer=0;
            }
        }
    }
}

bool hashjoin::probing(FILE *myFile,FILE *outPut,block_t *bigBuffer,int num,FILE *allFiles[],int nextPos[],int numOfFiles[],int nmem_blocks,char field)
{
    int outPutPointer=0;
    int nextId=1;
    int hashCode;
    int numOfRead=num/(nmem_blocks-2);
    int upol=num%(nmem_blocks-2);
    for(int i=0; i<numOfRead; i++)
    {
        fseek(myFile,sizeof(block_t)*(i)*(nmem_blocks-2),SEEK_SET);
        fread(bigBuffer,sizeof(block_t),nmem_blocks-2,myFile);
        numofios++;
        for(int k=0; k<nmem_blocks-2; k++)
        {
            for(int j=0; j<bigBuffer[k].nreserved; j++)
            {
                hashCode=hashFunction(bigBuffer[k].entries[j],num,nmem_blocks,field);
                checkTheBin(outPut,bigBuffer[k].entries[j],hashCode,bigBuffer,nmem_blocks,allFiles,outPutPointer,nextId,numOfFiles,field);
            }
        }

    }
    if(upol!=0)
    {
        fseek(myFile,sizeof(block_t)*(numOfRead),SEEK_SET);
        fread(bigBuffer,sizeof(block_t),upol,myFile);
        numofios++;
        for(int k=0; k<upol; k++)
        {
            for(int j=0; j<bigBuffer[k].nreserved; j++)
            {
                //cout<<"duo "<<j<<endl;
                hashCode=hashFunction(bigBuffer[k].entries[j],num,nmem_blocks,field);
                checkTheBin(outPut,bigBuffer[k].entries[j],hashCode,bigBuffer,nmem_blocks,allFiles,outPutPointer,nextId,numOfFiles,field);
            }
        }
    }
    if(outPutPointer>0)
    {
        bigBuffer[nmem_blocks-1].nreserved=outPutPointer;
        bigBuffer[nmem_blocks-1].blockid=nextId++;
        fwrite(&bigBuffer[nmem_blocks-1],sizeof(block_t),1,outPut);
        numofpairs+=outPutPointer;
        numofios++;
        outPutPointer=0;
    }
    fclose(outPut);


}


int hashjoin::dohashjoin(char* file1,char* file2,char* output,int numBlocksInFirstFile,int numBlocksInSecondFile,int nmem_blocks,char field,block_t *bigBuffer,unsigned int *nres, unsigned int *nios)
{
    numofpairs=0;
    numofios=0;
    FILE *rFile1=fopen(file1,"rb");
    FILE *rFile2=fopen(file2,"rb");
    FILE *outFile=fopen(output,"wb");
    FILE * allFiles[nmem_blocks-2]= {0};
    int nextPos[nmem_blocks-2]= {0};
    int numOfFiles[nmem_blocks-2]= {0};
    createTempFiles(allFiles,nmem_blocks);
    if(numBlocksInFirstFile<=numBlocksInSecondFile)
    {
        createTheHashTalble(rFile1,bigBuffer,numBlocksInFirstFile,allFiles,nextPos,numOfFiles,nmem_blocks,field);
        changePointers(allFiles,nmem_blocks);
        probing(rFile2,outFile,bigBuffer,numBlocksInSecondFile,allFiles,nextPos,numOfFiles,nmem_blocks,field);
    }
    else
    {
        createTheHashTalble(rFile2,bigBuffer,numBlocksInSecondFile,allFiles,nextPos,numOfFiles,nmem_blocks,field);
        changePointers(allFiles,nmem_blocks);
        probing(rFile1,outFile,bigBuffer,numBlocksInFirstFile,allFiles,nextPos,numOfFiles,nmem_blocks,field);
    }

    deleteTempFiles(allFiles,nmem_blocks);

    *nres=numofpairs;
    *nios=numofios;
}
