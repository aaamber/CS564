/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

//Variables should go in bTree.h
 
// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{

    //bufferMgr
    bufMgr = bufMgrIn;
    // attrByteOffset = attrByteOffset;
    attributeType = attrType;
    leafOccupancy = INTARRAYLEAFSIZE;
    nodeOccupancy = INTARRAYNONLEAFSIZE;
    scanExecuting = false;

    // Retrieve the Index Name
    std::ostringstream idxStr;
    idxStr << relationName << "." << attrByteOffset;
    outIndexName = idxStr.str();

    try
    {
	      // file exits
        file = new BlobFile(outIndexName, false);
        // read meta info
        headerPageNum = file->getFirstPageNo();
        Page *headerPage;
        bufMgr->readPage(file, headerPageNum, headerPage);
        IndexMetaInfo *meta = (IndexMetaInfo *)headerPage;
        rootPageNum = meta->rootPageNo;

        // check if index Info matches
        if (relationName != meta->relationName || attributeType != meta->attrType 
          || attrByteOffset != meta->attrByteOffset)
        {
          throw BadIndexInfoException(outIndexName);
        }
        // check if the root page is a leaf
        rootIsLeaf = rootPageNum == 2;

        bufMgr->unPinPage(file, headerPageNum, false);
        
    }
    //File was not found thus we create a new one
    catch(FileNotFoundException e)
    {
        //File did not exist from upon, thus create a new blob file
        file = new BlobFile(outIndexName, true);
        // allocate root and header page
        Page *headerPage;
        Page *rootPage;
        bufMgr->allocPage(file, headerPageNum, headerPage);
        bufMgr->allocPage(file, rootPageNum, rootPage);

        // fill meta infor
        IndexMetaInfo *meta = (IndexMetaInfo *)headerPage;
        meta->attrByteOffset = attrByteOffset;
        meta->attrType = attrType;
        meta->rootPageNo = rootPageNum;
        strncpy((char *)(&(meta->relationName)), relationName.c_str(), 20);
        meta->relationName[19] = 0;

        // initiaize root
        LeafNodeInt *root = (LeafNodeInt *)rootPage;
        root->rightSibPageNo = 0;
        rootIsLeaf = true;

        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);

        //fill the newly created Blob File using filescan
        FileScan fileScan(relationName, bufMgr);
        RecordId rid;
        try
        {
          // design choice 1: insert records one by one
          while(1)
          {
            fileScan.scanNext(rid);
            std::string record = fileScan.getRecord();
            // get key
            insertEntry(record.c_str() + attrByteOffset, rid);
          }

	        // design choice 2: sort file first and insert entries by sorted entry data

        }
        catch(EndOfFileException e)
        {
          std::cout<<"root page number is "<<rootPageNum<<std::endl;
          // printDataEntry();
          // save Btee index file to disk
          bufMgr->flushFile(file);
        }
    }

}

const void BTreeIndex::printDataEntry(){
    Page* Node;
    PageId curPN = 2;
    bufMgr->readPage(file, curPN, Node);
    std::cout<<"All key\n";
    LeafNodeInt *currNode;
    do{
        currNode = (LeafNodeInt*) Node;
        for(int i = 0; i < leafOccupancy; i++){
            if(currNode->keyArray[i] != 0){
            int value = currNode->keyArray[i];
            std::cout<<value<<"\n";}
        }
        bufMgr->unPinPage(file, curPN, false);
        curPN = currNode->rightSibPageNo;
        bufMgr->readPage(file, curPN, Node);
    }while(currNode->rightSibPageNo != 0);
    bufMgr->unPinPage(file, curPN, Node);
}
// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    scanExecuting = false;
    bufMgr->flushFile(BTreeIndex::file);
    delete file;
		file = nullptr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------


const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
  RIDKeyPair<int> dataEntry;
  dataEntry.set(rid, *((int *)key));
  // root
  Page* root;
  // PageId rootPageNum;
  bufMgr->readPage(file, rootPageNum, root);
  PageKeyPair<int> *newchildEntry = nullptr;
  insert(root, rootPageNum, rootIsLeaf ? true : false, dataEntry, newchildEntry);
}

const void BTreeIndex::findNextNonLeafNode(NonLeafNodeInt *curNode, PageId &nextNodeNum, int key)
{

  int i = nodeOccupancy;
  while(i >= 0 && (curNode->pageNoArray[i] == 0))
  {
    i--;
  }
  while(i > 0 && (curNode->keyArray[i-1] >= key))
  {
    i--;
  }
  nextNodeNum = curNode->pageNoArray[i];
}

const void BTreeIndex::insert(Page *curPage, PageId curPageNum, bool nodeIsLeaf, const RIDKeyPair<int> dataEntry, PageKeyPair<int> *&newchildEntry)
{
  // nonleaf node
  if (!nodeIsLeaf)
  {
    NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
    // find the right key to traverse
    Page *nextPage;
    PageId nextNodeNum;
    findNextNonLeafNode(curNode, nextNodeNum, dataEntry.key);
    bufMgr->readPage(file, nextNodeNum, nextPage);
    // NonLeafNodeInt *nextNode = (NonLeafNodeInt *)nextPage;
    nodeIsLeaf = curNode->level == 1;
    insert(nextPage, nextNodeNum, nodeIsLeaf, dataEntry, newchildEntry);
    
    // no split in child, just return
    if (newchildEntry == nullptr)
    {
	    // unpin current page from call stack
	    bufMgr->unPinPage(file, curPageNum, false);
    }
    else
	  {
      
      // if the curpage is not full
      if (curNode->pageNoArray[nodeOccupancy] == 0)
      {
        // insert the newchildEntry to curpage
        
        // std::cout<<"insert nonleaf key "<<newchildEntry->key<<std::endl;
        // std::cout<<"insert nonleaf key "<<(*newchildEntry).key<<std::endl;
        
        insertNonLeaf(curNode, newchildEntry);
        newchildEntry = nullptr;
        // finish the insert process, unpin current page
        bufMgr->unPinPage(file, curPageNum, true);
      }
      else
      {
        splitNonLeaf(curNode, curPageNum, newchildEntry);
      }
    }
  }
  else
  {
    LeafNodeInt *leaf = (LeafNodeInt *)curPage;
    // page is not full
    if (leaf->ridArray[leafOccupancy - 1].page_number == 0)
    {
      insertLeaf(leaf, dataEntry);
      bufMgr->unPinPage(file, curPageNum, true);
      newchildEntry = nullptr;
    }
    else
    {
      splitLeaf(leaf, curPageNum, newchildEntry, dataEntry);
      //std::cout<<"split leaf pushed key is "<< newchildEntry->key<<std::endl;
    }
  }
}
const void BTreeIndex::printKeys(std::string a, NonLeafNodeInt *node){
  int i = 0;
  while(i < nodeOccupancy && node->keyArray[i+1] != 0){
    std::cout<< a << node->keyArray[i]<< std::endl;
  }
}
const void BTreeIndex::splitNonLeaf(NonLeafNodeInt *oldNode, PageId oldPageNum, PageKeyPair<int> *&newchildEntry)
{
  // allocate a new nonleaf node
  PageId newPageNum;
  Page *newPage;
  bufMgr->allocPage(file, newPageNum, newPage);
  NonLeafNodeInt *newNode = (NonLeafNodeInt *)newPage;


  int mid = nodeOccupancy/2;
  int pushupIndex = mid;
  PageKeyPair<int> pushupEntry;
  // even number of keys
  if (nodeOccupancy % 2 == 0)
  {
      pushupIndex = newchildEntry->key < oldNode->keyArray[mid] ? mid -1 : mid;
  }
  pushupEntry.set(newPageNum, oldNode->keyArray[pushupIndex]);

  mid = pushupIndex + 1;
  // move half the entries to the new node
  for(int i = mid; i < nodeOccupancy; i++)
  {
    newNode->keyArray[i-mid] = oldNode->keyArray[i];
    newNode->pageNoArray[i-mid] = oldNode->pageNoArray[i+1];
    oldNode->pageNoArray[i+1] = (PageId) 0;
    oldNode->keyArray[i+1] = 0;
  }

  newNode->level = oldNode->level;
  // remove the entry that is pushed up from current node
  oldNode->keyArray[pushupIndex] = 0;
  oldNode->pageNoArray[pushupIndex] = (PageId) 0;
  // insert the new child entry
  insertNonLeaf(newchildEntry->key < newNode->keyArray[0] ? oldNode : newNode, newchildEntry);
  // newchildEntry = new PageKeyPair<int>();
  newchildEntry = &pushupEntry;
  printKeys("new key is ", newNode);
  printKeys("old key is ", oldNode);
  // reinterpret_cast<PageId>(newchildEntry = &pushupEntry);
  bufMgr->unPinPage(file, oldPageNum, true);
  bufMgr->unPinPage(file, newPageNum, true);

  // if the curNode is the root
  if (oldPageNum == rootPageNum)
  {
    updateRoot(oldPageNum, newchildEntry);
  }
}

const void BTreeIndex::updateRoot(PageId firstPageInRoot, PageKeyPair<int> *newchildEntry)
{
  // create a new root 
  PageId newRootPageNum;
  Page *newRoot;
  bufMgr->allocPage(file, newRootPageNum, newRoot);
  NonLeafNodeInt *newRootPage = (NonLeafNodeInt *)newRoot;

  // update metadata
  newRootPage->level = rootIsLeaf ? 1 : 0;
  newRootPage->pageNoArray[0] = firstPageInRoot;
  newRootPage->pageNoArray[1] = newchildEntry->pageNo;
  newRootPage->keyArray[0] = newchildEntry->key;

  Page *meta;
  bufMgr->readPage(file, headerPageNum, meta);
  IndexMetaInfo *metaPage = (IndexMetaInfo *)meta;
  metaPage->rootPageNo = newRootPageNum;
  rootPageNum = newRootPageNum;
  // unpin unused page
  bufMgr->unPinPage(file, headerPageNum, true);
  bufMgr->unPinPage(file, newRootPageNum, true);
}

const void BTreeIndex::splitLeaf(LeafNodeInt *leaf, PageId leafPageNum, PageKeyPair<int> *&newchildEntry, const RIDKeyPair<int> dataEntry)
{
  // allocate a new leaf page
  PageId newPageNum;
  Page *newPage;
  bufMgr->allocPage(file, newPageNum, newPage);
  LeafNodeInt *newLeafNode = (LeafNodeInt *)newPage;

  int mid = leafOccupancy/2;
  // copy half the page to newLeafNode
  for(int i = 0; i < mid; i++)
  {
    newLeafNode->keyArray[i] = leaf->keyArray[mid+i];
    newLeafNode->ridArray[i] = leaf->ridArray[mid+i];
    leaf->keyArray[mid+i] = 0;
    leaf->ridArray[mid+i].page_number = 0;
  }
  if (dataEntry.key < newLeafNode->keyArray[0])
  {
    insertLeaf(leaf, dataEntry);
  }
  else
  {
    insertLeaf(newLeafNode, dataEntry);
  }

  // update sibling pointer
  newLeafNode->rightSibPageNo = leaf->rightSibPageNo;
  leaf->rightSibPageNo = newPageNum;

  // the smallest key from second page as the new child entry
  newchildEntry = new PageKeyPair<int>();
  PageKeyPair<int> newKeyPair;
  newKeyPair.set(newPageNum, newLeafNode->keyArray[0]);
  newchildEntry = &newKeyPair;
  //std::cout<< "in split leaf, push key is "<<newchildEntry->key<<std::endl;
  bufMgr->unPinPage(file, leafPageNum, true);
  bufMgr->unPinPage(file, newPageNum, true);

  // if curr page is root
  if (leafPageNum == rootPageNum)
  {
    updateRoot(leafPageNum, newchildEntry);
    rootIsLeaf = false;
  }
}

const void BTreeIndex::insertLeaf(LeafNodeInt *leaf, RIDKeyPair<int> entry)
{
  // empty leaf page
  if (leaf->ridArray[0].page_number == 0)
  {
    leaf->keyArray[0] = entry.key;
    leaf->ridArray[0] = entry.rid;    
  }
  else
  {
    int i = leafOccupancy - 1;
    // find the end
    while(i >= 0 && (leaf->ridArray[i].page_number == 0))
    {
      i--;
    }
    // shift entry
    while(i >= 0 && (leaf->keyArray[i] > entry.key))
    {
      leaf->keyArray[i+1] = leaf->keyArray[i];
      leaf->ridArray[i+1] = leaf->ridArray[i];
      i--;
    }
    // insert entry
    leaf->keyArray[i+1] = entry.key;
    leaf->ridArray[i+1] = entry.rid;
  }
}

const void BTreeIndex::insertNonLeaf(NonLeafNodeInt *nonleaf, PageKeyPair<int> *entry)
{
  
  //std::cout<<"insertNonleaf key is "<<entry->key<<std::endl;
  int i = nodeOccupancy;
  while(i >= 0 && (nonleaf->pageNoArray[i] == 0))
  {
    i--;
  }
  // shift
  while( i > 0 && (nonleaf->keyArray[i-1] > entry->key))
  {
    nonleaf->keyArray[i] = nonleaf->keyArray[i-1];
    //std::cout << "key is "<<nonleaf->keyArray[i]<<std::endl;
    nonleaf->pageNoArray[i+1] = nonleaf->pageNoArray[i];
    i--;
  }
  // insert
  nonleaf->keyArray[i] = entry->key;
  nonleaf->pageNoArray[i+1] = entry->pageNo;
  // std::cout<<"insertNonleaf key is "<<entry->key<<std::endl;
   
  // test
  //printKeys("afterInsert ", nonleaf);
}


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

/**
 * Begin a filtered scan of the index.  For instance, if the method is called 
 * using ("a",GT,"d",LTE) then we should seek all entries with a value 
 * greater than "a" and less than or equal to "d".
 * If another scan is already executing, that needs to be ended here.
 * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
 * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
 * @param lowVal  Low value of range, pointer to integer / double / char string
 * @param lowOp   Low operator (GT/GTE)
 * @param highVal High value of range, pointer to integer / double / char string
 * @param highOp  High operator (LT/LTE)
 * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values 
 * @throws  BadScanrangeException If lowVal > highval
 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
**/

const void BTreeIndex::startScan(const void* lowValParm,
           const Operator lowOpParm,
           const void* highValParm,
           const Operator highOpParm)
{
  
  lowValInt = *((int *)lowValParm);
  highValInt = *((int *)highValParm);

  if(!((lowOpParm == GT or lowOpParm == GTE) and (highOpParm == LT or highOpParm == LTE)))
  {
    throw BadOpcodesException();
  }
  if(lowValInt > highValInt)
  {
    throw BadScanrangeException();
  }

  lowOp = lowOpParm;
  highOp = highOpParm;

  // Scan is already started
  if(scanExecuting)
  {
    endScan();
  }

  currentPageNum = rootPageNum;
  // Start scanning by reading rootpage into the buffer pool
  bufMgr->readPage(file, currentPageNum, currentPageData);

  if(!rootIsLeaf)
  {
    // Cast
    NonLeafNodeInt* currentNode = (NonLeafNodeInt *) currentPageData;
    bool foundLeaf = false;
    while(!foundLeaf)
    {
      // Cast page to node
      //std::cout<<"currentPageNum is "<<currentPageNum<<"\n";
      currentNode = (NonLeafNodeInt *) currentPageData;
      // Check if this is the level above the leaf, if yes, the next level is the leaf
      if(currentNode->level == 1)
      {
        foundLeaf = true;
      }

      // Find the leaf
      PageId nextPageNum;
      findNextNonLeafNode(currentNode, nextPageNum, lowValInt);
      //std::cout<<"nextpage is "<<nextPageNum<<"\n";
      // Unpin
      bufMgr->unPinPage(file, currentPageNum, false);
      currentPageNum = nextPageNum;
      // read the nextPage
      bufMgr->readPage(file, currentPageNum, currentPageData);
    }
  }
  // Now the curNode is leaf node try to find the smallest one that satisefy the OP
  bool found = false;
  while(!found){
    // Cast page to node
    LeafNodeInt* currentNode = (LeafNodeInt *) currentPageData;
    //int printOutData = currentNode->keyArray[0];
    //std::cout<<"first data check is"<< printOutData<<"\n";
    // Check if the whole page is null
    if(currentNode->ridArray[0].page_number == 0)
    {
      bufMgr->unPinPage(file, currentPageNum, false);
      throw NoSuchKeyFoundException();
    }
    // Search from the left leaf page to the right to find the fit
    bool nullVal = false;
    for(int i = 0; i < leafOccupancy and !nullVal; i++)
    {
      int key = currentNode->keyArray[i];
      //std::cout<<"Key in the loop is "<<key<<"\n";
      // Check if the next one in the key is not inserted
      if(i < leafOccupancy - 1 and currentNode->ridArray[i + 1].page_number == 0)
      {
        nullVal = true;
      }
      
      if(_satisfies(lowValInt, lowOp, highValInt, highOp, key))
      {
        // select
        //std::cout<<"key is selected is "<<key<< "\n";
        nextEntry = i;
        found = true;
        scanExecuting = true;
        break;
      }
      else if((highOp == LT and key >= highValInt) or (highOp == LTE and key > highValInt))
      {
        bufMgr->unPinPage(file, currentPageNum, false);
        throw NoSuchKeyFoundException();
      }
      
      // Did not find any matching key in this leaf, go to next leaf
      if(i == leafOccupancy - 1 or nullVal){
        //unpin page
        bufMgr->unPinPage(file, currentPageNum, false);
        //did not find the matching one in the more right leaf
        if(currentNode->rightSibPageNo == 0)
        {
          throw NoSuchKeyFoundException();
        }
        currentPageNum = currentNode->rightSibPageNo;
        bufMgr->readPage(file, currentPageNum, currentPageData);
      }
    }
  }
}

/**
  * Fetch the record id of the next index entry that matches the scan.
  * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
  * @param outRid RecordId of next record found that satisfies the scan criteria returned in this
  * @throws ScanNotInitializedException If no scan has been initialized.
  * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
**/
const void BTreeIndex::scanNext(RecordId& outRid) 
{
  if(!scanExecuting)
  {
    throw ScanNotInitializedException();
  }
	// Cast page to node
	LeafNodeInt* currentNode = (LeafNodeInt *) currentPageData;
  if(currentNode->ridArray[nextEntry].page_number == 0 or nextEntry == leafOccupancy)
  {
    // Unpin page and read papge
    bufMgr->unPinPage(file, currentPageNum, false);
    // No more next leaf
    if(currentNode->rightSibPageNo == 0)
    {
      throw IndexScanCompletedException();
    }
    currentPageNum = currentNode->rightSibPageNo;
    bufMgr->readPage(file, currentPageNum, currentPageData);
    currentNode = (LeafNodeInt *) currentPageData;
    // Reset nextEntry
    nextEntry = 0;
  }


 
  // Check  if rid satisfy
  int key = currentNode->keyArray[nextEntry];
  if(_satisfies(lowValInt, lowOp, highValInt, highOp, key))
  {
    outRid = currentNode->ridArray[nextEntry];
    // Incrment nextEntry
    nextEntry++;
    // If current page has been scanned to its entirety
  }
  else
  {
    throw IndexScanCompletedException();
  }
}

/**
  * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
  * @throws ScanNotInitializedException If no scan has been initialized.
**/
const void BTreeIndex::endScan() 
{
  if(!scanExecuting)
  {
    throw ScanNotInitializedException();
  }
  scanExecuting = false;
  // Unpin page
  bufMgr->unPinPage(file, currentPageNum, false);
  // Reset variable
  currentPageData = nullptr;
  currentPageNum = static_cast<PageId>(-1);
  nextEntry = -1;
}

/**
  * Helper function to check if the key is satisfies
  * @param lowVal   Low value of range, pointer to integer / double / char string
  * @param lowOp    Low operator (GT/GTE)
  * @param highVal  High value of range, pointer to integer / double / char string
  * @param highOp   High operator (LT/LTE)
  * @param val      Value of the key
  * @return True if satisfies False if not
  *
**/
const bool BTreeIndex::_satisfies(int lowVal, const Operator lowOp, int highVal, const Operator highOp, int val)
{
  if(lowOp == GTE && highOp == LTE)
  {
    return val >= lowVal && val <= highVal;
  }
  else if(lowOp == GT && highOp == LTE)
  {
    return val > lowVal && val <= highVal;
  }
  else if(lowOp == GTE && highOp == LT)
  {
    return val >= lowVal && val < highVal;
  }
  else
  {
    return val > lowVal && val < highVal;
  }
}

}
