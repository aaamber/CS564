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
       
}

void BufMgr::advanceClock()
{
   //increment clockhand%bufs

   clockHand = (clockHand + 1)%bufs;
   //cihange refbit = 0 if pin count = 0
}

void BufMgr::allocBuf(FrameId & frame) 
{    
    //throw exception if all frames are pinned
    for(FrameId i = 0; i < numBufs; i++)
    {
       if (!bufDescTable[clockHand].valid)
       {
         
         Set(, bufDescTable[clockHand].pageNo);

       } else
       {
          if(bufDescTable[clockHand].refbit){
            bufDescTable[clockHand].refbit = false;
            advanceClock();
            continue;
          }else{
            if(bufDescTable[clockHand].pinCnt == 0){
              if(bufDesTable[clockHand].dirty){
                 writePage();
                 hashtableUpdate();
                 bufDesTableUpdate();
                 //TODO update everything!

              }else{
                 Set();
              }
            }else{
               advanceClock();
            }
          }
       }
    }
    //TODO throw exception
    //if there is an unpinned frame/
       //get the frame, check dirty first--> write to disk
    //else
       
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    //do lookup in hashtable and catch exception to handle case 1
    //else case 2
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::flushFile(const File* file) 
{

}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
    page = file.allocatePage();
    pageNo = page.page_number();
    FrameId *frame;
    allocBuf(frame);
    hashTable.insert(file, pageNo, frame);
    Set(file, pageNo);
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
   try{
      int frameNo;
      hashTable.lookup(file, PageNo, &frameNo);
      bufDescTable[frameNo].valid = false;
      hashTable.remove(file, pageNo);
   } catch (HashNotFoundException e){
      file.deletePage(PageNo)
   }
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
