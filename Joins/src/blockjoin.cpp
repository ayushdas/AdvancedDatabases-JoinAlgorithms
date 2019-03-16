#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "minirel.h"
#include "heapfile.h"
#include "scan.h"
#include "join.h"
#include "relation.h"
#include "bufmgr.h"

//---------------------------------------------------------------
// BlockNestedLoopJoin
//
// Input:   specOfR
//          specOfS
//          B
// Output:  None
// Return:  Pin requests, pin misses and total running time
//          of algorithm
// Purpose: Performs a Block Nested Loops join on relations R and S;
//          collects performance statistics
// 
// Notes:
// SpecOfR and SpecOfS specify which relations we are going to 
// join, which attributes we are going to join on, the offsets 
// of the attributes etc. specOfR specifies the outer relation, 
// while specOfS specifies the inner relation. B specifies the 
// size of the block in terms of the number of records.
// 
// The result of the join is placed into a temporary HeapFile.
//
// You can use MakeNewRecord() to create the new result record.
//
// Remember to clean up before exiting by "delete"ing any pointers
// that you "new"ed.  This includes any Scan that you have opened.
//---------------------------------------------------------------

void BlockNestedLoopJoin(JoinSpec specOfR, JoinSpec specOfS, int B, long& pinRequests, long& pinMisses, double& duration)
{
	MINIBASE_BM->ResetStat(); // Reset the buffer manager
	clock_t start = clock(); // start the clock
	Status status = OK;
    // Create a new heapfile to store join result
    HeapFile *jointResult = new HeapFile(NULL, status); 
	if (status != OK){
        cerr << "ERROR: Cannot create new HeapFile for the join result.\n";
    }    
    // Scan relation R
    status = OK;
	Scan *scan_R = specOfR.file->OpenScan(status);
	if (status != OK) {
        cerr << "ERROR : Cannot open scan on the relation R.\n";
    }	
	RecordID rid_R, rid_S, rid_Res; // Record IDs Definition
    // Record pointers Declaration
	char *recPtr_R = new char[specOfR.recLen];
	char *recPtr_S = new char[specOfS.recLen];
    char *recPtr_Res = new char[specOfR.recLen + specOfS.recLen];    
	// To keep track of blocks
	bool blocksDone = false; 
	// B represents the number of tuples of the outer relation
	// It is the size of the array storing the outer relation
	int recordsPerRBlock = B; // Records per block
	int block_size = B * specOfR.recLen; // Size of the block in bytes
	char *recPtr_Block = new char[block_size];
	while(!blocksDone) 	// Iterate over the blocks
	{
		int RRead = 0;
		for(int i = 0; i < recordsPerRBlock; i++)
		{
			if( scan_R->GetNext( rid_R, recPtr_Block + i*specOfR.recLen, specOfR.recLen) != OK)
			{
				blocksDone = true;
				break;
			}
			RRead++;
		}
		Scan *scan_S = specOfS.file->OpenScan(status);
		if (status != OK) {
            cerr << "ERROR : Cannot open scan on the relation S.\n";	
        }	
		while( scan_S->GetNext( rid_S, recPtr_S, specOfS.recLen) == OK)
		{
			for(int i = 0; i < RRead; i++)
			{
				memcpy(recPtr_R, recPtr_Block + i * specOfR.recLen, specOfR.recLen);
                int* join_col_R = (int*)&recPtr_R[specOfR.offset];
				int* join_col_S = (int*)&recPtr_S[specOfS.offset];
				if(*join_col_R == *join_col_S)
				{
					MakeNewRecord( recPtr_Res, recPtr_R, recPtr_S, specOfR.recLen, specOfS.recLen);	
					jointResult -> InsertRecord( recPtr_Res, specOfR.recLen + specOfS.recLen, rid_Res);
				}
			}
		}
		delete scan_S;
	}
    delete jointResult;
    delete scan_R;
	delete[] recPtr_R;
	delete[] recPtr_S;
	delete[] recPtr_Block;
	MINIBASE_BM->GetStat(pinRequests, pinMisses);
	duration = ( clock() - start) / (double) CLOCKS_PER_SEC;
}
