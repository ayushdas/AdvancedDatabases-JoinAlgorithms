#include <stdlib.h>
#include <iostream>
#include <time.h>
#include <ctype.h>

#include "minirel.h"
#include "bufmgr.h"
#include "heapfile.h"
#include "join.h"
#include "relation.h"

int MINIBASE_RESTART_FLAG = 0;// used in minibase part

#define NUM_OF_DB_PAGES  2000 // define # of DB pages
#define NUM_OF_BUF_PAGES 50 // define Buf manager size.You will need to change this for the analysis

int main()
{
	comparisonOfTwoJoinAlgos();
}

void comparisonOfTwoJoinAlgos()
{
	// int count = 0;
	// double *avgPinNo = new double[2];
	// double *avgPinMisses = new double[3];
	// double *avgDuration = new double[3];
	double avgPinMisses_tuple = 0.0;
	double avgDuration_tuple = 0.0;
	int numOfRepitions = 10;
	//printf("%s\n", ">>----------Test 1: Origional settings----------");
	// cout << "Timg Test 1";
	// printSettings(NUM_OF_BUF_PAGES, NUM_OF_REC_IN_R, NUM_OF_REC_IN_S);

	for(int numOfRepetition = 0; numOfRepetition < numOfRepitions; numOfRepetition++)
	{

		// do joins
		// long *pinNo = new long[3];
		
		long *pinMisses = new long[2];
		double *duration = new double[2];

		callJoins( NUM_OF_BUF_PAGES, NUM_OF_REC_IN_R, NUM_OF_REC_IN_S, pinMisses, duration);
	    cout << pinMisses[0];
		cout << duration[0];
		avgPinMisses_tuple += pinMisses[0];
		avgDuration_tuple += duration[0];
		
		// for(int i = 0; i < 3; i++)
		// {
		// 	// avgPinNo[i] = (avgPinNo[i] * numOfRepetition + pinNo[i]) / (numOfRepetition + 1);
		// 	avgPinMisses[i] = (avgPinMisses[i] * numOfRepetition + pinMisses[i]) / (numOfRepetition + 1);
		// 	avgDuration[i] = (avgDuration[i] * numOfRepetition + duration[i]) / (numOfRepetition + 1);
		// }

		delete[] pinNo;
		delete[] pinMisses;
		delete[] duration;
		// count++;
	}
	cout<< avgPinMisses_tuple/numOfRepetition;
	// printResults( avgPinNo, avgPinMisses, avgDuration);
	// delete[] avgPinNo, avgPinMisses, avgDuration;
	
	// printTestTitle(1, false, "Origional settings");
}

void callJoins( int numOfBuf, int numOfRecR, int numOfRecS, 
	 long pinMisses[2], double duration[2] )
{
	remove( "MINIBASE.DB" ); 		
	Status s;

	// Create a database manager
	minibase_globals = new SystemDefs(s, 
		"MINIBASE.DB",
		"MINIBASE.LOG",
		NUM_OF_DB_PAGES,   // Number of pages allocated for database
		500,
		numOfBuf,  // Number of frames in buffer pool
		NULL);

	srand(1);

	// Create relation R and S
	cerr << "Creating random records for relation R\n";
	CreateR();
	cerr << "Creating random records for relation S\n";
	CreateS();

	JoinSpec specOfS, specOfR;
	CreateSpecForR(specOfR);
	CreateSpecForS(specOfS);

	// int blocksize = (MINIBASE_BM->GetNumOfUnpinnedBuffers()-3*3)*MINIBASE_PAGESIZE;

	TupleNestedLoopJoin(specOfR, specOfS, pinMisses[0], duration[0]);
	// BlockNestedLoopJoin(specOfR, specOfS, blocksize, pinMisses[1], duration[1]);

	delete minibase_globals;
}
// void printTestTitle(int testNo, bool isStart, const char* nameOfTest)
// {
// 	FILE *pFile = fopen ("data.txt","a");
// 	if(isStart)
// 	{
// 		printf("%s%d%s%s%s\n", ">>----------Test ", testNo, ": ", nameOfTest, "----------");
// 		fprintf(pFile, "%s%d%s%s%s\n", ">>----------Test ", testNo, ": ", nameOfTest, "----------");
// 	}
// 	else
// 	{
// 		printf("%s%d%s%s%s\n\n", ">>----------End of Test ", testNo, ": ", nameOfTest, "----------");
// 		fprintf(pFile, "%s%d%s%s%s\n\n", ">>----------End of Test ", testNo, ": ", nameOfTest, "----------");
// 	}

// 	fclose(pFile);
// }

// void printSettings(int buf, int recR, int recS)
// {
// 	FILE *pFile = fopen ("data.txt","a");
// 	printf("%-10s%15s%15s%15s\n", "Settings", "# Buf Pages", "# Rec in R", "# Rec in S"); 
// 	printf("%25d%15d%15d\n", buf, recR, recS); 
// 	fprintf(pFile, "%-10s%15s%15s%15s\n", "Settings", "# Buf Pages", "# Rec in R", "# Rec in S");
// 	fprintf(pFile, "%25d%15d%15d\n", buf, recR, recS); 
// 	fclose (pFile);
// }

// void printResults(double avgPinNo[3], double avgPinMisses[3], double avgDuration[3])
// {
// 	FILE *pFile = fopen ("data.txt","a");
// 	printf("%-10s%15s%15s%15s\n", "Results", "Avg # Pin", "Avg # Misses", "Avg Duration"); 
// 	printf("%-10s%15.0f%15.0f%15f\n", "Tuple Join", avgPinNo[0], avgPinMisses[0], avgDuration[0]); 
// 	printf("%-10s%15.0f%15.0f%15f\n", "Block Join", avgPinNo[1], avgPinMisses[1], avgDuration[1]); 
// 	printf("%-10s%15.0f%15.0f%15f\n\n", "Index Join", avgPinNo[2], avgPinMisses[2], avgDuration[2]); 
// 	fprintf(pFile, "%-10s%15s%15s%15s\n", "Results", "Avg # Pin", "Avg # Misses", "Avg Duration"); 
// 	fprintf(pFile, "%-10s%15.0f%15.0f%15f\n", "Tuple Join", avgPinNo[0], avgPinMisses[0], avgDuration[0]); 
// 	fprintf(pFile, "%-10s%15.0f%15.0f%15f\n", "Block Join", avgPinNo[1], avgPinMisses[1], avgDuration[1]); 
// 	fprintf(pFile, "%-10s%15.0f%15.0f%15f\n\n", "Index Join", avgPinNo[2], avgPinMisses[2], avgDuration[2]); 
// 	fclose(pFile);
// }
