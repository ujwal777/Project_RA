#include "global.h"

/**
 * @brief 
 * SYNTAX: CROSS_TRANPOSE matrix1 matrix2
 */
bool syntacticParseCROSSTRANSPOSE() {
    logger.log("syntacticParseCROSSTRANSPOSE");
     if (tokenizedQuery.size() != 3)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.ismatrix=true;
    parsedQuery.queryType = CROSSTRANSPOSE;
    parsedQuery.crossTransFirstMatrix = tokenizedQuery[1];
    parsedQuery.crossTransSecondMatrix = tokenizedQuery[2];
    return true;

}

bool semanticParseCROSSTRANSPOSE() {
    logger.log("semanticParseCROSSTRANSPOSE");
    if (!tableCatalogue.isTable(parsedQuery.crossTransFirstMatrix) || !tableCatalogue.isTable(parsedQuery.crossTransSecondMatrix))
    {
        cout << "SEMANTIC ERROR: Cross Transpose Relations don't exist" << endl;
        return false;
    }

    Table table1 = *(tableCatalogue.getTable(parsedQuery.crossTransFirstMatrix));
    Table table2 = *(tableCatalogue.getTable(parsedQuery.crossTransSecondMatrix));

    if(table1.rowCount != table2.rowCount || table1.columnCount != table2.columnCount)
    {
        cout << "SEMANTIC ERROR: Cross Transpose Relations don't have same dimensions" << endl;
        return false;
    }
    return true;
}

void executeCROSSTRANSPOSE() {
    
    logger.log("executeCROSSTRANSPOSE");
    Table table1 = *(tableCatalogue.getTable(parsedQuery.crossTransFirstMatrix));
    Table table2 = *(tableCatalogue.getTable(parsedQuery.crossTransSecondMatrix));

    // logger.log("Page::writePage");
    // ofstream fout1(parsedQuery.crossTransSecondMatrix, ios::out);
    // ofstream fout2(parsedQuery.crossTransSecondMatrix, ios::out);
    vector<int> pageindex;
    for(int i=0;i<table2.rowCount;i++){
        pageindex.push_back(i*table2.columnCount/table2.maxElementsperblock);
        // cout<<pageindex[pageindex.size()-1]<<" ";
    }
    if(parsedQuery.crossTransFirstMatrix==parsedQuery.crossTransSecondMatrix){
         Table *tablenew = new Table(parsedQuery.crossTransSecondMatrix);
         tablenew->tableName=parsedQuery.crossTransFirstMatrix+"_dup";
          tableCatalogue.insertTable(tablenew);
         tablenew->load();
    }
    int curr=0;
    int ind=0;
    Cursor cursor1(table1.tableName, 0);
    cursor1.transposeLine(0);
    // cout << "cross transpose" << endl;
    return;
}