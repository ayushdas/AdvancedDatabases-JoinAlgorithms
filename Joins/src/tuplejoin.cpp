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
// TupleNestedLoopJoin
//
// Input:   specOfR
//          specOfS
// Output:  None
// Return:  Pin requests, pin misses and total running time
//          of algorithm
// Purpose: Performs a nested loop join on relations R and S,
//          a tuple a time; collects performance statistics
//
// Notes:
// SpecOfR and SpecOfS specify which relations we are going to 
// join, which attributes we are going to join on, the offsets 
// of the attributes etc. specOfR specifies the outer relation 
// while specOfS specifies the inner relation.
//
// The result of the join is placed into a temporary HeapFile.
//
// You can use MakeNewRecord() to create the new result record.
//
// Remember to clean up before exiting by "delete"ing any pointers
// that you "new"ed. This includes any Scan that you have opened.
//---------------------------------------------------------------


void TupleNestedLoopJoin(JoinSpec specOfR, JoinSpec specOfS, long& pinRequests, long& pinMisses, double& duration)
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
    if (status != OK){
        cerr << "ERROR : Cannot open scan on the relation R.\n";
    }
    // Record IDs Definition
    RecordID rid_R, rid_S, rid_Res;
    // Record ptr Declaration
    char *recPtr_R = new char[specOfR.recLen];
    char *recPtr_S = new char[specOfS.recLen];
    char *recPtr_Res = new char[specOfR.recLen + specOfS.recLen];
    while(scan_R -> GetNext(rid_R, recPtr_R, specOfR.recLen) == OK){
        // Scan Relation on S
        Scan *scan_S = specOfS.file->OpenScan(status);
        if (status != OK){
        cerr << "ERROR: Cannot open scan on the relation S.\n";
        }
        int* join_col_R = (int*) &recPtr_R[specOfR.offset]
        while(scan_S -> GetNext(rid_S, recPtr_S, specOfS.recLen) == OK){             
            int* join_col_S = (int*) &recPtr_S[specOfS.offset]
            if (*join_col_R == *join_col_S){
                MakeNewRecord( recPtr_Res, recPtr_R, recPtr_S, specOfR.recLen, specOfS.recLen);
                jointResult -> InsertRecord(recPtr_Res, specOfR.recLen+specOfS.recLen, rid_Res);	
            }
        }
        delete scan_S;
    }
    delete jointResult;
    delete scan_R;
    delete[] recPtr_R, recPtr_S, recPtr_Res;
    MINIBASE_BM->GetStat(pinRequests, pinMisses); 
    duration = ( clock() - start) / (double) CLOCKS_PER_SEC;
}
