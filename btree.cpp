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

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
  RIDKeyPair<int> dataEntry = RIDKeyPair<int>(rid, *((int *)key));
  // root
  Page* root;
  PageId rootPageNum;
  bufMgr->readPage(file, rootPageNum, root);
  

}

// TODO initialize rootIsLeaf
void BtreeINdex::insert(Page *curPage, PageId curPageNum, const RIDKeyPair<int> dataEntry, Page *newchildEntry)
{
  // nonleaf node
  if (curPageNum == rootPageNum)
  {
    // if the root is a leaf
    if (rootIsleaf)
    {
      LeafNodeInt *root = (LeafNodeInt *)curPage;
      // page is not full
      if (root->ridArray[leafOccupancy - 1] == 0)
      {
        insertLeaf(root, dataEntry);
        bufMgr->unPinPage(file, curPageNum, true);
      }
      else
      {
        
      }
    }
  }
  else
  {
        
  }
}


void BtreeIndex::insertLeaf(LeafNodeInt *leaf, RIDKeyPair<int> entry)
{
  // empty leaf page
  if (leaf->ridArray[0] == 0)
  {
    leaf->keyArray[0] = entry.key;
    leaf->ridArray[0] = entry.rid;    
  }
  else
  {
    int i = leafOccupancy - 1;
    // find the end
    while(i >= 0 && (leaf->ridArray[i] == 0))
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

void BtreeIndex::insertNonLeaf(NonLeafNodeInt *nonleaf, PageKeyPair<int> entry)
{
  int i = nodeOccupancy;
  while(i >= 0 && (nonleaf->pageNoArray[i] == 0))
  {
    i--;
  }
  // shift
  while( i > 0 && (nonleaf->keyArray[i-1] > entry.key))
  {
    nonleaf->keyArray[i] = nonleaf->keyArray[i-1];
    nonleaf->pageNoArray[i+1] = nonleaf->pageNoArray[i];
    i--;
  }
  // insert
  nonleaf->keyArray[i] = entry.key;
  nonleaf->pageNoArray[i+1] = entry.pageNo;
}
// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
