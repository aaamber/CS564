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
  RIDKeyPair<int> dataEntry;
  dataEntry.set(rid, *((int *)key));
  // root
  Page* root;
  PageId rootPageNum;
  bufMgr->readPage(file, rootPageNum, root);
  // TODO: find a way to detect whether the root is a leaf node
  if (rootIsLeaf)
  {
    insert(root, rootPageNum, true, dataEntry);
  }
  else
  {
    insert(root, rootPageNum, false, dataEntry);
  }
}

// TODO initialize rootIsLeaf
void BTreeIndex::insert(Page *curPage, PageId curPageNum, bool nodeIsLeaf, const RIDKeyPair<int> dataEntry, PageKeyPair *newchildEntry)
{
  // nonleaf node
  if (!nodeIsLeaf)
  {
    NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
    // find the right key to traverse
    Page *nextNode;
    PageId *nextNodeNum;
    int i = nodeOccupancy;
    while(i >= 0 && (curNode->pageNoArray[i] == 0))
    {
      i--;
    }
    while(i > 0 && (curNode->keyArray[i-1] >= dataEntry.key))
    {
      i--;
    }
    nextNodeNum = curNode->pageNoArray[i+1];
    bufMgr->readPage(file, nextNodeNum, nextNode);
    nextNode = (NonLeafNodeInt *)nextNode;

    if (curNode->level == 1)
    {
      // next page is leaf
      nodeIsLeaf = true;
    }
    else
    {
      nodeIsLeaf = false;
    }
    insert(nextNode, nextNodeNum, nodeIsLeaf, dataEntry, newchildEntry);

    // no split in child, just return
    if (newchildEntry == NULL) return;
    else
    {
      // if the curpage is not full
      if (curNode->pageNoArray[nodeOccupancy] == 0)
      {
        // insert the newchildEntry to curpage
        insertNonLeaf(curNode, newchildEntry);
        newchildEntry = NULL;
        // finish the insert process, unpin current page
        bufMgr->unPinPage(file, curPageNum, true);
      }
      else
      {
        splitNonLeaf(curNode, curPageNum, newchildEntry);
      }
      return;
    }

  }
  else
  {
    LeafNodeInt *leaf = (LeafNodeInt *)curPage;
    // page is not full
    if (leaf->ridArray[leafOccupancy - 1] == 0)
    {
      insertLeaf(leaf, dataEntry);
      bufMgr->unPinPage(file, curPageNum, true);
      newchildEntry = NULL;
      return;
    }
    else
    {
      splitLeaf(curPage, curPageNum, newchildEntry, dataEntry);
      return;
    }
  }
}

void BTreeIndex::splitNonLeaf(NonLeafNodeInt *oldNode, PageId oldPageNum, PageKeyPair<int> *newchildEntry)
{
  // allocate a new nonleaf node
  PageId newPageNum;
  Page *newPage;
  bufMgr->allocPage(file, newPageNum, newPage);
  NonLeafNodeInt *newNode = (NonLeafNodeInt *)newPage;

  // move half the entries to the new node
  int mid = nodeOccupancy/2;
  for(int i = 0; i < mid; i++)
  {
    newNode->keyArray[i] = oldNode->keyArray[mid+i];
    newNode->pageNoArray[i] = oldNode->pageNoArray[mid+1+i];
    oldNode->pageNoArray[mid+1+i] = 0;
  }
  newNode->level = oldNode->level;

  // insert the new child entry
  if (newchildEntry->key < newNode->keyArray[0])
  {
    insertNonLeaf(oldNode, newchildEntry);
  }
  else
  {
    insertNonLeaf(newNode, newchildEntry);
  }

  PageKeyPair<int> newKeyPair;
  newKeyPair.set(newPageNum, newNode->keyArray[0])
  newchildEntry = &newKeyPair;
  bufMgr->unPinPage(file, oldPageNum, true);
  bufMgr->unPinPage(file, newPageNum, true);

  // if the curNode is the root
  if (oldPageNum == rootPageNum)
  {
    updateRoot(oldPageNum);
  }
}

void BTreeIndex::updateRoot(PageId firstPageInRoot)
{
  // create a new root 
  PageId newRootPageNum;
  Page *newRoot;
  bufMgr->alloPage(file, newRootPageNum, newRoot);
  NonLeafNodeInt *newRootPage = (NonLeafNodeInt *)newRoot;

  // update metadata
  newRootPage->level = 1;
  newRootPage->pageNoArray[0] = firstPageInRoot;

  Page *meta;
  bufMgr->readPage(file, headerPageNum, meta);
  IndexMetaInfo *metaPage = (IndexMetaInfo *)meta;
  metaPage->rootPageNo = newRootPageNum;
  rootPageNum = newRootPageNum;
  // unpin unused page
  bufMgr->unPinPage(file, headerPageNum, true);
  bufMgr->unPinPage(file, newRootPageNum, true);
}

void BTreeIndex::splitLeaf(LeafNodeInt *leaf, PageId leafPageNum, PageKeyPair *newchildEntry, const RIDKeyPair<int> dataEntry)
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
    leaf->ridArray[mid+i] = 0
  }
  if (dataEntry->key < newLeafNode->keyArray[0])
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
  PageKeyPair<int> newKeyPair;
  newKeyPair.set(newPageNum, newLeafNode->keyArray[0]);
  newchildEntry = &newKeyPair;

  bufMgr->unPinPage(file, leafPageNum, true);
  bufMgr->unPinPage(file, newPageNum, true);

  // if curr page is root
  if (leafPageNum == rootPageNum)
  {
    updateRoot(leafPageNum);
  }
}

void BTreeIndex::insertLeaf(LeafNodeInt *leaf, RIDKeyPair<int> entry)
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

void BTreeIndex::insertNonLeaf(NonLeafNodeInt *nonleaf, PageKeyPair<int> entry)
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
