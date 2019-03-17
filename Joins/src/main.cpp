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
#define NUM_OF_BUF_PAGES 50 //50 // define Buf manager size.You will need to change this for the analysis
// Buffer Pool Page sizes 25,50,75,100
void callJoins( int numOfBuf, int numOfRecR, int numOfRecS, long pinNo[2], 
	 long pinMisses[2], double duration[2] );
void callJoins_with_swap( int numOfBuf, int numOfRecR, int numOfRecS, long pinNo[4], 
	 long pinMisses[4], double duration[4] );
void comparisonOfTwoJoinAlgos();
void changingTheBufferPoolSize();
void interchanging_call_joins();
int main()
{
	//comparisonOfTwoJoinAlgos();
	//changingTheBufferPoolSize();
	interchanging_call_joins();	
}
void interchanging_call_joins()
{

	double *avgPinMisses = new double[4];avgPinMisses[0]=avgPinMisses[1]=avgPinMisses[2]=avgPinMisses[3]=0.0;
	double *avgDuration = new double[4];avgDuration[0]=avgDuration[1]=avgDuration[2]=avgDuration[3]=0.0;
	int numOfRepitions = 5;
	for(int numOfRepetition = 0; numOfRepetition < numOfRepitions; numOfRepetition++)
	{

		// do joins
		long *pinNo = new long[4];
		long *pinMisses = new long[4];
		double *duration = new double[4];

		callJoins_with_swap(NUM_OF_BUF_PAGES, NUM_OF_REC_IN_R, NUM_OF_REC_IN_S,pinNo,pinMisses, duration);
		cout<<"*****************************"<<endl;
		cout<<"Number of Buffer Pages are: "<<NUM_OF_BUF_PAGES<<endl;
		cout<<"The number of records in R are: "<<NUM_OF_REC_IN_R<<"\n";
		cout<<"The number of records in S are: "<<NUM_OF_REC_IN_S<<"\n";
		cout << "Statistics for Tuple Join\n";
	    	cout <<numOfRepetition+" Number of Page Misses =  "<<pinMisses[0]<<endl;
		cout <<numOfRepetition+" Duration of Iteration = "<<duration[0]<<endl;
		cout << "Statistics for Block Join\n";
	    	cout <<numOfRepetition+" Number of Page Misses =  "<<pinMisses[1]<<endl;
		cout <<numOfRepetition+" Duration of Iteration = "<<duration[1]<<endl;
		cout << "Statistics for interchanged Tuple Join\n";
	    	cout <<numOfRepetition+" Number of Page Misses =  "<<pinMisses[2]<<endl;
		cout <<numOfRepetition+" Duration of Iteration = "<<duration[2]<<endl;
		cout << "Statistics for interchanged Block Join\n";
	    	cout <<numOfRepetition+" Number of Page Misses =  "<<pinMisses[3]<<endl;
		cout <<numOfRepetition+" Duration of Iteration = "<<duration[3]<<endl;
		avgPinMisses[0] += pinMisses[0];
		avgDuration[0] += duration[0];
		avgPinMisses[1] += pinMisses[1];
		avgDuration[1] += duration[1];
		avgPinMisses[2] += pinMisses[2];
		avgDuration[2] += duration[2];
		avgPinMisses[3] += pinMisses[3];
		avgDuration[3] += duration[3];
		cout<<"*****************************"<<endl;	
		delete[] pinNo;
		delete[] pinMisses;
		delete[] duration;
	}
	cout<<"*** Average Statistics ***";
		cout << "Statistics for Tuple Join\n";
	    	cout <<"Average"<<" Number of Page Misses =  "<<avgPinMisses[0]/numOfRepitions<<endl;
		cout <<"Average"<<" Duration of Iteration = "<<avgDuration[0]/numOfRepitions<<endl;
		cout << "Statistics for Block Join\n";
	    	cout <<"Average"<<" Number of Page Misses =  "<<avgPinMisses[1]/numOfRepitions<<endl;
		cout <<"Average"<<" Duration of Iteration = "<<avgDuration[1]/numOfRepitions<<endl;
		cout << "Statistics for Interchanged Tuple Join\n";
	    	cout <<"Average"<<" Number of Page Misses =  "<<avgPinMisses[2]/numOfRepitions<<endl;
		cout <<"Average"<<" Duration of Iteration = "<<avgDuration[2]/numOfRepitions<<endl;
		cout << "Statistics for Interchanged Block Join\n";
	    	cout <<"Average"<<" Number of Page Misses =  "<<avgPinMisses[3]/numOfRepitions<<endl;
		cout <<"Average"<<" Duration of Iteration = "<<avgDuration[3]/numOfRepitions<<endl;
		delete[] avgPinMisses;delete[] avgDuration;
}
void callJoins_with_swap( int numOfBuf, int numOfRecR, int numOfRecS, long pinNo[4], 
	 long pinMisses[4], double duration[4] )
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
	int blocksize = 50;
	TupleNestedLoopJoin(specOfR, specOfS,pinNo[0], pinMisses[0], duration[0]);
	BlockNestedLoopJoin(specOfR, specOfS, blocksize, pinNo[1] ,pinMisses[1], duration[1]);
	TupleNestedLoopJoin(specOfS, specOfR,pinNo[2], pinMisses[2], duration[2]);
	BlockNestedLoopJoin(specOfS, specOfR, blocksize, pinNo[3] ,pinMisses[3], duration[3]);
	delete minibase_globals;
}
void changingTheBufferPoolSize()
{
	// int count = 0;
	// double *avgPinNo = new double[2];
	double *avgPinMisses = new double[2];avgPinMisses[0]=0.0;
	double *avgDuration = new double[2];avgDuration[0]=0.0;
	//double avgPinMisses_tuple = 0.0;
	//double avgDuration_tuple = 0.0;
	int numOfRepitions = 5;
	for(int numOfRepetition = 0; numOfRepetition < numOfRepitions; numOfRepetition++)
	{

		// do joins
		long *pinNo = new long[2];
		long *pinMisses = new long[2];
		double *duration = new double[2];

		callJoins( NUM_OF_BUF_PAGES, NUM_OF_REC_IN_R, NUM_OF_REC_IN_S,pinNo,pinMisses, duration);
		cout<<"*****************************"<<endl;
		cout<<"Number of Buffer Pages are: "<<NUM_OF_BUF_PAGES<<endl;
		cout<<"The number of records in R are: "<<NUM_OF_REC_IN_R<<"\n";
		cout<<"The number of records in S are: "<<NUM_OF_REC_IN_S<<"\n";
		cout << "Statistics for Tuple Join\n";
	    	cout <<numOfRepetition+" Number of Page Misses =  "<<pinMisses[0]<<endl;
		cout <<numOfRepetition+" Duration of Iteration = "<<duration[0]<<endl;
		cout << "Statistics for Block Join\n";
	    	cout <<numOfRepetition+" Number of Page Misses =  "<<pinMisses[1]<<endl;
		cout <<numOfRepetition+" Duration of Iteration = "<<duration[1]<<endl;
		avgPinMisses[0] += pinMisses[0];
		avgDuration[0] += duration[0];
		avgPinMisses[1] += pinMisses[1];
		avgDuration[1] += duration[1];
		cout<<"*****************************"<<endl;	
		delete[] pinNo;
		delete[] pinMisses;
		delete[] duration;
	}
	cout<<"*** Average Statistics ***";
		cout << "Statistics for Tuple Join\n";
	    	cout <<"Average"<<" Number of Page Misses =  "<<avgPinMisses[0]/numOfRepitions<<endl;
		cout <<"Average"<<" Duration of Iteration = "<<avgDuration[0]/numOfRepitions<<endl;
		cout << "Statistics for Block Join\n";
	    	cout <<"Average"<<" Number of Page Misses =  "<<avgPinMisses[1]/numOfRepitions<<endl;
		cout <<"Average"<<" Duration of Iteration = "<<avgDuration[1]/numOfRepitions<<endl;
		delete[] avgPinMisses;delete[] avgDuration;
}


void comparisonOfTwoJoinAlgos()
{
	double *avgPinMisses = new double[2];avgPinMisses[0]=0.0;
	double *avgDuration = new double[2];avgDuration[0]=0.0;
	int numOfRepitions = 5;
	for(int numOfRepetition = 0; numOfRepetition < numOfRepitions; numOfRepetition++)
	{

		long *pinNo = new long[2];
		long *pinMisses = new long[2];
		double *duration = new double[2];

		callJoins( NUM_OF_BUF_PAGES, NUM_OF_REC_IN_R, NUM_OF_REC_IN_S,pinNo,pinMisses, duration);
		cout<<"*****************************"<<endl;
		cout<<"Number of Buffer Pages are: "<<NUM_OF_BUF_PAGES<<endl;
		cout<<"The number of records in R are: "<<NUM_OF_REC_IN_R<<"\n";
		cout<<"The number of records in S are: "<<NUM_OF_REC_IN_S<<"\n";
		cout << "Statistics for Tuple Join\n";
	    	cout <<numOfRepetition+" Number of Page Misses =  "<<pinMisses[0]<<endl;
		cout <<numOfRepetition+" Duration of Iteration = "<<duration[0]<<endl;
		cout << "Statistics for Block Join\n";
	    	cout <<numOfRepetition+" Number of Page Misses =  "<<pinMisses[1]<<endl;
		cout <<numOfRepetition+" Duration of Iteration = "<<duration[1]<<endl;
		avgPinMisses[0] += pinMisses[0];
		avgDuration[0] += duration[0];
		avgPinMisses[1] += pinMisses[1];
		avgDuration[1] += duration[1];
		cout<<"*****************************"<<endl;	
		delete[] pinNo;
		delete[] pinMisses;
		delete[] duration;
	}
	cout<<"*** Average Statistics ***";
		cout << "Statistics for Tuple Join\n";
	    	cout <<"Average"<<" Number of Page Misses =  "<<avgPinMisses[0]/numOfRepitions<<endl;
		cout <<"Average"<<" Duration of Iteration = "<<avgDuration[0]/numOfRepitions<<endl;
		cout << "Statistics for Block Join\n";
	    	cout <<"Average"<<" Number of Page Misses =  "<<avgPinMisses[1]/numOfRepitions<<endl;
		cout <<"Average"<<" Duration of Iteration = "<<avgDuration[1]/numOfRepitions<<endl;
		delete[] avgPinMisses;delete[] avgDuration;
}

void callJoins( int numOfBuf, int numOfRecR, int numOfRecS, long pinNo[2], 
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

	int blocksize = 50;
	TupleNestedLoopJoin(specOfR, specOfS,pinNo[0], pinMisses[0], duration[0]);
	BlockNestedLoopJoin(specOfR, specOfS, blocksize, pinNo[1] ,pinMisses[1], duration[1]);

	delete minibase_globals;
}
