/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
    //if exist frame that is pined, throw PagePinnedException
    for (FrameId i = 0; i < numBufs; i++)
    {
        if (bufDescTable[i].pinCnt != 0){
            throw PagePinnedException("page pinned", bufDescTable[i].pageNo, i);
        }
    }
    //if all frame is not pined, flush out the dirty page
    for (FrameId i = 0; i < numBufs; i++){
        if(bufDescTable[i].dirty != 0){//page is dirty
            const Page new_page = bufPool[i];
            bufDescTable[i].file->writePage(new_page);
        }
    }
    //deallocate buffer pool
    delete bufPool;
    delete bufDescTable;
}

void BufMgr::advanceClock()
{
   //increment clockhand%bufs
   clockHand = (clockHand + 1)%bufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{   

    int count = 0;
    while(count <= numBufs){
        advanceClock();
        if (!bufDescTable[clockHand].valid)
        {
            frame = bufDescTable[clockHand].frameNo;
            return;
        }
        if(bufDescTable[clockHand].refbit)
        {
            bufDescTable[clockHand].refbit = false;
            continue;
        }
        if(bufDescTable[clockHand].pinCnt == 0)
        {
            break;
        }else
        {
            count++;
        }
    }
    if(count > numsBufs)
    {
        throw BUfferExceededException();
    }

    // write to disk if dirty
    if(bufDescTable[clockHand].dirty)
    {
        bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
    }else
    {
        frame = bufDescTable[clockHand].frameNo;
    }
    //remove relevent hashtable entry
    hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);   
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    //do lookup in hashtable and catch exception to handle case 1
    //else case 2
    FrameId* frame;
    try
    {
	hashTable->lookup(file, pageNo, *frame);
        //set ref bit
        bufDescTable[*frame].refbit = true;
        //increase pin count
        bufDescTable[*frame].pinCnt += 1;
        //TODO return pointer to frame via page
    }
    catch(HashNotFoundException e){
        //if page not found in hash table
        //allocate buffer frame
        allocBuf(*frame);
        //readPage
        *page = file->readPage(pageNo);
        //insert into hash table
        hashTable->insert(file,pageNo,*frame);
        //set page
        bufDescTable[*frame].Set(file,pageNo);
        //TODO return pointer to frame via page
    }
    catch (BufferExceededException e)
    {
        std::cerr << "BufferExceededException in readPage()" << "\n";
    }
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
    FrameId frameNo;
    //do nothing if throw exception
    try{
        hashTable->lookup(file, pageNo, frameNo);
        if (bufDescTable[frameNo].pinCnt ==0){
            throw PageNotPinnedException("page not pinned", pageNo, frameNo);
        }
        else{
            bufDescTable[frameNo].pinCnt--;
        }
    }
    catch (HashNotFoundException e){
         std::cerr << "HashNotFoundException in unpinPage()" << "\n";
    }
}

void BufMgr::flushFile(const File* file) 
{
    //loop the frame table once to find any excpetion
    for (FrameId i = 0; i < numBufs; i++)
    {
        if (bufDescTable[i].file == file){
            //Throw PagePinnedException if one page is pined
            if (bufDescTable[i].pinCnt != 0){
                throw PagePinnedException("page not pinned", bufDescTable[i].pageNo, i);
            }
            //Throw BadBufferException if an invalid page belonging to
            //the file is encountered.
            if (bufDescTable[i].valid == 0){
                throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
            }
        }
    }
    //scan bufTable for pages belonging to the file
    for (FrameId i = 0; i < numBufs; i++)
    {
        if (bufDescTable[i].file == file){
            //if the page is dirty, call file->writePage() to flush
            //the page to disk and then set the dirty bit for the
            //page to false
            if (bufDescTable[i].dirty != 0){
                const Page new_page = bufPool[i];
                bufDescTable[i].file->writePage(new_page);
                bufDescTable[i].dirty = 0;
            }
            //remove the page from the hashtable
            hashTable->remove(file, bufDescTable[i].pageNo);
            //invoke the Clear() method of BufDesc for the page frame.
            bufDescTable[i].Clear();
        }
    }
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
    //TODO call allocBuf first? need to delete the allocated page if failed?
    page = file->allocatePage();
    pageNo = page.page_number();
    FrameId frameNo;
    try
    {
        allocBuf(frameNo);
        hashTable->insert(file, pageNo, frameNo);
        Set(file, pageNo);
        bufDescTable[frameNo].Set(file, pageNo);
        bufPool[frameNo] = page;
    }
    catch(BufferExceededException e)
    {
        std::cerr << "BufferExceededException in allocPage()" << "\n";
    }
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    //if the page to be deleted is allocated a frame in the buffer pool,
    //that frame is freed and correspondingly entry from hash table is
    //also removed
    FrameId frameNo;
    try{
        hashTable->lookup(file, PageNo, frameNo);
        bufDescTable[frameNo].Clear();// not sure if it the free frame
        hashTable->remove(file, PageNo);
    }
    catch(HashNotFoundException e){
    }
    //delete page from the file
    file->deletePage(PageNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
