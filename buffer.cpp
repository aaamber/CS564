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

/*
ECE 564
Group 32:
Nicholas LePar
Wei Li
Xiaofei Liu
*/

namespace badgerdb { 

/*
*BufMgr constructor: creates initial buffer pool with the number of frames
*defined by the param bufs. Sets the clock. Creates the hash table for page
*to buffer hash relations.
*
*@param bufs	number of frames in the buffer
*/
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


/*
*~BufMgr deconstructor: attempts to deallocate the entire buffer pool
*writes all dirty buffers.
*@throws pagePinnedExeption if there is a page currently in use
*/
BufMgr::~BufMgr() {
    //if exist frame that is pined, throw PagePinnedException
    for (FrameId i = 0; i < numBufs; i++)
    {
        if (bufDescTable[i].pinCnt != 0)
	{
            throw PagePinnedException("page pinned", bufDescTable[i].pageNo, i);
        }
    }
    //if all frame is not pined, flush out the dirty page
    for (FrameId i = 0; i < numBufs; i++)
    {
        if(bufDescTable[i].dirty != 0)
	{//page is dirty
            bufDescTable[i].file->writePage(bufPool[i]);
        }
    }
    //deallocate buffer pool, DescTable and hashTable
    delete[] bufPool;
    delete[] bufDescTable;
    delete hashTable;
}

/*advanceClock : increments the clock hand to the next apporiate spot*/
void BufMgr::advanceClock()
{
   //increment clockhand%bufs
   clockHand = (clockHand + 1)%numBufs;
}
/**
* allocBuf: Allocate a free frame. Searchs through bufferPool for an avaliable frame.
* @param frame   Frame reference, frame ID of allocated frame returned via this variable
* @throws BufferExceededException If no such buffer is found which can be allocated
*/
void BufMgr::allocBuf(FrameId & frame) 
{   
    uint32_t count = 0;
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
    if(count > numBufs)
    {
        throw BufferExceededException();
    }

    // write to disk if dirty
    if(bufDescTable[clockHand].dirty)
    {
        bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
    	frame = bufDescTable[clockHand].frameNo;
    }else
    {
        frame = bufDescTable[clockHand].frameNo;
    }
    //remove relevent hashtable entry
    hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);   
    //clear the buffer for use (Test 5)
    bufDescTable[clockHand].Clear();
}

	
/**
* Reads the given page from the file into a frame and returns the pointer to page.
* If the requested page is already present in the buffer pool pointer to that frame is returned
* otherwise a new frame is allocated from the buffer pool for reading the page.
*
* @param file   	File object
* @param PageNo  Page number in the file to be read
* @param page  	Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.*/
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    FrameId frame;
    //Read a page in the buffer, return it through "page", and update buffer info.
    try{
        //check to see if page is in table
	hashTable->lookup(file, pageNo, frame);
        //set ref bit
        bufDescTable[frame].refbit = true;
        //increase pin count
        bufDescTable[frame].pinCnt += 1;
        //return pointer to the frame
	page = &bufPool[frame];
    }catch(const HashNotFoundException& e){
        //if page not found in hash table
        //allocate buffer frame
        allocBuf(frame);
	//increment diskreads stats
        bufStats.diskreads++;
	//readPage directly into bufPool[frame]
        bufPool[frame]= file->readPage(pageNo);
        //set page
        bufDescTable[frame].Set(file,pageNo);
	//return pointer to the frame
        page=&bufPool[frame];
	//insert into the hashTable since page is now in buffer
        hashTable->insert(file,pageNo,frame);
    }
    catch (BufferExceededException e)
    {
        std::cerr << "BufferExceededException in readPage()" << "\n";
    }
}


/**
* Unpin a page from memory since it is no longer required for it to remain in memory.
*
* @param file   	File object
* @param PageNo  Page number
* @param dirty		True if the page to be unpinned needs to be marked dirty	
* @throws  PageNotPinnedException If the page is not already pinned*/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
    FrameId frameNo;
    //do nothing if throw exception
    try{
        hashTable->lookup(file, pageNo, frameNo);
        if (bufDescTable[frameNo].pinCnt == 0)
	{
            throw PageNotPinnedException("page not pinned", pageNo, frameNo);
        }
        else{
            bufDescTable[frameNo].pinCnt--;
            if (dirty)
	    {
                bufDescTable[frameNo].dirty = true;
            }
        }
    }
    catch (HashNotFoundException e)
    {
         std::cerr << "HashNotFoundException in unpinPage()" << "\n";
    }
}


/**
* Writes out all dirty pages of the file to disk.
* All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
* Otherwise Error returned.
*
* @param file   	File object
* @throws  PagePinnedException If any page of the file is pinned in the buffer pool 
* @throws BadBufferException If any frame allocated to the file is found to be invalid
*/
void BufMgr::flushFile(const File* file) 
{
    //loop the frame table once to find any excpetion
    for (FrameId i = 0; i < numBufs; i++)
    {
        if (bufDescTable[i].file == file)
	{
            //Throw PagePinnedException if one page is pined
            if (bufDescTable[i].pinCnt != 0)
	    {
                throw PagePinnedException("page not pinned", bufDescTable[i].pageNo, i);
            }
            //Throw BadBufferException if an invalid page belonging to
            //the file is encountered.
            if (bufDescTable[i].valid == 0)
	    {
                throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
            }
        }
    }
    //scan bufTable for pages belonging to the file
    for (FrameId i = 0; i < numBufs; i++)
    {
        if (bufDescTable[i].file == file)
	{
            //if the page is dirty, call file->writePage() to flush
            //the page to disk and then set the dirty bit for the
            //page to false
            if (bufDescTable[i].dirty != 0)
	    {
                bufDescTable[i].file->writePage(bufPool[i]);
                bufDescTable[i].dirty = 0;
            }
            //remove the page from the hashtable
            hashTable->remove(file, bufDescTable[i].pageNo);
            //invoke the Clear() method of BufDesc for the page frame.
            bufDescTable[i].Clear();
        }
    }
}


/**
* Allocates a new, empty page in the file and returns the Page object.
* The newly allocated page is also assigned a frame in the buffer pool.
* may recieve an exception if buffer size is exceeded. 
*
* @param file   	File object
* @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
* @param page  	Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.*/
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
    FrameId frameNo;
    //Allocate buffer frame
    allocBuf(frameNo);
    //create a new page at the frame in the buffer
    bufPool[frameNo] = file->allocatePage();
    //return a pointer to the frame
    page=&bufPool[frameNo]; 
    //set the pageNo 
    pageNo = page->page_number();
    //set the frame in the buffer
    bufDescTable[frameNo].Set(file, pageNo);
    //insert into the hash table`
    hashTable->insert(file, pageNo, frameNo);

}


/**
* Delete page from file and also from buffer pool if present.
* Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
*
* @param file   	File object
* @param PageNo  Page number
*/
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
