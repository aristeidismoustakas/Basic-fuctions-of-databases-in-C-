#include "mergesort.h"
using namespace std;

unsigned int npasses_mg = 0;
unsigned int nios_mg = 0;
unsigned int nsorted_segs = 0;
bool isLastLevel = false;

mergesort::mergesort()
{
    //ctor
}

mergesort::~mergesort()
{
    //dtor
}
int mergesort::compareNum (const void * a, const void * b)
{
    record_t * recA=(record_t *) a;
    record_t * recB=(record_t *) b;

    return (recA->num - recB->num);
}
int mergesort::compareId(const void * a, const void * b)
{
    record_t * recA=(record_t *) a;
    record_t * recB=(record_t *) b;

    return (recA->recid - recB->recid);
}
int mergesort::compareString(const void * a, const void * b)
{
    record_t * recA=(record_t *) a;
    record_t * recB=(record_t *) b;

    return (strcmp(recA->str,recB->str));
}
int mergesort::compareBoth(const void * a, const void * b)
{
    record_t * recA=(record_t *) a;
    record_t * recB=(record_t *) b;

    if(!strcmp(recA->str,recB->str))
    {
        return (recA->num - recB->num);
    }
    else
    {
        return (strcmp(recA->str,recB->str));
    }
}


int mergesort::sortBlocks(block_t *buffer,int num,char field)
{
    for(int i=0; i<num; i++)
    {
        switch (field)
        {
        case '0':
            qsort(buffer[i].entries,MAX_RECORDS_PER_BLOCK,sizeof(record_t),compareId);
            break;
        case '1':
            qsort(buffer[i].entries,MAX_RECORDS_PER_BLOCK,sizeof(record_t),compareNum);
            break;
        case '2':
            qsort(buffer[i].entries,MAX_RECORDS_PER_BLOCK,sizeof(record_t),compareString);
            break;
        case '3':
            qsort(buffer[i].entries,MAX_RECORDS_PER_BLOCK,sizeof(record_t),compareBoth);
            break;

        }

    }

}

bool mergesort::isFinished(int  *pointers,block_t *buffer,int nmem_numblocks,int level,FILE *rFile,int *lastRead,int k,int notUsed,int partFill)
{
    int usable = nmem_numblocks-1-notUsed;
    for (int j=0; j<usable-1; j++)  //elegxos an kapoios buffer exei adeiasei
    {
        if(pointers[j]==buffer[usable-1].nreserved)
        {
            pointers[j] = -1;
        }
    }
    int ep=1;
    for(int lev=1; lev<=level; lev++)
    {
        ep=ep*(nmem_numblocks-1);
    }
    int h=ep/(nmem_numblocks-1);

    if(pointers[usable-1] == buffer[usable-1].nreserved) pointers[usable-1] = -1;
    for (int j=0; j<usable-1; j++)                // Mporoume kai kateu8eian molis ginei -1 fortwnoume
    {
        if(pointers[j]==-1 && level>1 && lastRead[j]<h)
        {
            //trava apo mnimi
            pointers[j] = 0;
            fseek(rFile,sizeof(block_t)*(h*j+lastRead[j]+k),SEEK_SET);
            fread(&buffer[j],sizeof(block_t),1,rFile);
            nios_mg++;
            lastRead[j]++;
        }
    }
    if(pointers[usable-1]==-1 && level>1 && lastRead[usable-1]<h-partFill)
    {
        pointers[usable-1] = 0;
        fseek(rFile,sizeof(block_t)*(h*(usable-1)+lastRead[usable-1]+k),SEEK_SET);
        fread(&buffer[usable-1],sizeof(block_t),1,rFile);
        nios_mg++;
        lastRead[usable-1]++;
    }
    for(int i=0; i<usable; i++)
    {
        if(pointers[i]!=-1) return false;        //elegxos an oloi oi buufers einai adeioi.
    }
    return true;
}

bool mergesort::merging(block_t *buffer,int nmem_numblocks,char field,int numBlocksInFile,FILE *pFile,int &counter,int level,FILE *rFile,int k,int notUsed,int partFill)
{
    int pointers[nmem_numblocks-1-notUsed]= {0};
    int lastRead[nmem_numblocks-1-notUsed];
    std::fill_n(lastRead, nmem_numblocks-1-notUsed, 1);

    int outPutPointer=0;
    int from=0;
    if(level==1)    sortBlocks(buffer,nmem_numblocks-1-notUsed,field);
    while(!isFinished(pointers,buffer,nmem_numblocks,level,rFile,lastRead,k,notUsed,partFill))
    {

        from=0;
        for (int j=0; j<nmem_numblocks-1-notUsed; j++)
        {
            if(pointers[j]!=-1)
            {
                from = j;
                break;
            }
        }

        record_t mini = buffer[from].entries[pointers[from]];
        for(int i=0; i<nmem_numblocks-1-notUsed; i++)
        {
            switch (field)
            {
            case '0':
                if(buffer[i].entries[pointers[i]].recid<mini.recid && pointers[i]!=-1)
                {
                    mini=buffer[i].entries[pointers[i]];
                    from=i;
                }
                break;
            case '1':
                if(buffer[i].entries[pointers[i]].num<mini.num && pointers[i]!=-1)
                {
                    mini=buffer[i].entries[pointers[i]];
                    from=i;
                }
                break;
            case '2':
                if(strcmp(buffer[i].entries[pointers[i]].str,mini.str)<0 && pointers[i]!=-1)
                {
                    mini=buffer[i].entries[pointers[i]];
                    from=i;
                }
                break;
            case '3':
                if(strcmp(buffer[i].entries[pointers[i]].str,mini.str)==0 && pointers[i]!=-1)
                {
                    if(buffer[i].entries[pointers[i]].num<mini.num)
                    {
                        mini=buffer[i].entries[pointers[i]];
                        from=i;
                    }
                }
                else if(strcmp(buffer[i].entries[pointers[i]].str,mini.str)<0 && pointers[i]!=-1)
                {
                    mini=buffer[i].entries[pointers[i]];
                    from=i;
                }
                break;
            }
        }

        buffer[from].entries[pointers[from]].rdummy1 = 99;
        pointers[from] ++;
        //grapse se arxeio
        if(outPutPointer == MAX_RECORDS_PER_BLOCK )
        {
            //midenise pointer
            buffer[nmem_numblocks-1].blockid=counter++;
            buffer[nmem_numblocks-1].nreserved = MAX_RECORDS_PER_BLOCK;
            fwrite(&buffer[nmem_numblocks-1],sizeof(block_t),1,pFile);
            if(isLastLevel) nsorted_segs++;
            nios_mg++;
            outPutPointer = 0;

        }
        buffer[nmem_numblocks-1].entries[outPutPointer] = mini;
        outPutPointer++;
    }
    buffer[nmem_numblocks-1].blockid=counter++;
    buffer[nmem_numblocks-1].nreserved = outPutPointer;
    fwrite(&buffer[nmem_numblocks-1],sizeof(block_t),1,pFile);  //grafei to teleytaio block poy  einai fortwmeno ston buffer.
    if(isLastLevel) nsorted_segs++;
    nios_mg++;
}


bool mergesort::do_mergesort(const char* input,const char* output,int numBlocksInFile,int nmem_blocks,char field,unsigned int *nsorted_segments,unsigned int *npasses,unsigned int *nios)
{
    npasses_mg = 0;
    nios_mg = 0;
    nsorted_segs = 0;
    isLastLevel = false;
    block_t *bigBuffer=new block_t[nmem_blocks];
    FILE *rFile;
    int c;
    int notUsed;
    int partFill;
    rFile=fopen(input,"rb");
    FILE *pFile=fopen(output,"wb");
    int counter=0;
    notUsed = numBlocksInFile%(nmem_blocks - 1);
    if(notUsed!=0)
    {
        notUsed = nmem_blocks - 1 - notUsed;
    }
    for (int i=0; i<numBlocksInFile; i=i+nmem_blocks-1)
    {
        fread(bigBuffer,sizeof(block_t),nmem_blocks-1,rFile);
        nios_mg++;
        merging(bigBuffer,nmem_blocks,field,numBlocksInFile,pFile,counter,1,rFile,0,0,0);
    }
    if(notUsed!=0)
    {
        fread(bigBuffer,sizeof(block_t),nmem_blocks-1-notUsed,rFile);
        nios_mg++;
        merging(bigBuffer,nmem_blocks,field,numBlocksInFile,pFile,counter,1,rFile,0,notUsed,0);

    }
    fclose(rFile);
    fclose(pFile);
    if(numBlocksInFile<=nmem_blocks) return true;
    block_t buffer1;

    bool finish=false;
    int reqLevel = 2;
    while(!finish)
    {
        if(pow(nmem_blocks-1,reqLevel) >= numBlocksInFile)
        {
            finish = true;
        }
        else reqLevel++;
    }
    FILE *tFile;
    npasses_mg = reqLevel;
    for(int l=2; l<reqLevel+1; l++)
    {
        if(l==reqLevel) isLastLevel = true;
        counter=0;
        if(l%2==0)
        {
            tFile=fopen("temp.bin","wb");
            rFile=fopen(output,"rb");
        }
        else
        {
            rFile=fopen("temp.bin","rb");
            tFile=fopen( output,"wb");
        }
        int ep=1;
        for(int lev=1; lev<=l; lev++)
        {
            ep=ep*(nmem_blocks-1);
        }
        int h=ep/(nmem_blocks-1);
        notUsed = 0;
        int upoloipa=0;
        int k;
        bool isLast=false;
        for(k=0; k<numBlocksInFile; k=k+ep)
        {
            c=0;
            if((k+ep)>numBlocksInFile)
                isLast=true;
            if(!isLast)
            {
                for(int j=0; j<nmem_blocks-1; j++)
                {
                    fseek(rFile,sizeof(block_t)*(j*h+k),SEEK_SET);
                    fread(&bigBuffer[c],sizeof(block_t),1,rFile);
                    nios_mg++;
                    c++;
                }
                merging(bigBuffer,nmem_blocks,field,numBlocksInFile,tFile,counter,l,rFile,k,notUsed,0);
            }
            else
            {
                upoloipa=numBlocksInFile-k;
                if(upoloipa>nmem_blocks-1)
                {
                    int loops=upoloipa/h;
                    bool isExact=false;
                    if(upoloipa%h==0)
                        isExact=true;
                    int aa=0;
                    int q;
                    for(q=0; q<loops; q++)
                    {
                        fseek(rFile,sizeof(block_t)*(q*h+k),SEEK_SET);
                        fread(&bigBuffer[c],sizeof(block_t),1,rFile);
                        nios_mg++;
                        c++;
                    }
                    notUsed=nmem_blocks-1-loops;
                    partFill=0;
                    if(!isExact)
                    {
                        partFill=(loops+1)*h-upoloipa;
                        fseek(rFile,sizeof(block_t)*(q*h+k),SEEK_SET);
                        fread(&bigBuffer[c],sizeof(block_t),1,rFile);
                        nios_mg++;
                        notUsed--;
                        merging(bigBuffer,nmem_blocks,field,numBlocksInFile,tFile,counter,l,rFile,k,notUsed,partFill);
                    }
                    else
                    {
                        merging(bigBuffer,nmem_blocks,field,numBlocksInFile,tFile,counter,l,rFile,k,notUsed,partFill);
                    }
                }
                else
                {
                    fseek(rFile,sizeof(block_t)*(k),SEEK_SET);
                    fread(bigBuffer,sizeof(block_t),upoloipa,rFile);
                    nios_mg++;
                    sortBlocks(bigBuffer,upoloipa,field);
                    fwrite(bigBuffer,sizeof(block_t),upoloipa,tFile);
                    if(isLastLevel) nsorted_segs+=upoloipa;
                    nios_mg++;

                }
            }
        }
        fclose(tFile);
        fclose(rFile);
    }

    if(reqLevel%2 == 0)
    {
        remove(output);
        rename("temp.bin",output);
    }
    else
    {
        remove("temp.bin");
    }
    *nios = nios_mg;
    *npasses = npasses_mg;
    *nsorted_segments = nsorted_segs;
}


