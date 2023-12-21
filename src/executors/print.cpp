#include "global.h"
/**
 * @brief 
 * SYNTAX: PRINT relation_name
 */
bool syntacticParsePRINT()
{
    logger.log("syntacticParsePRINT");
    if ((tokenizedQuery.size() == 2 && tokenizedQuery[1]!="MATRIX") || (tokenizedQuery.size()==3 && tokenizedQuery[1]=="MATRIX"))
    {
        if(tokenizedQuery[1]=="MATRIX"){
            parsedQuery.ismatrix=true;
            parsedQuery.printRelationName = tokenizedQuery[2];

        }
        else{
            parsedQuery.ismatrix=false;
            parsedQuery.printRelationName = tokenizedQuery[1];
        }
        parsedQuery.queryType = PRINT;
        return true;
       
    }
    else{
        cout << "SYNTAX ERROR" << endl;
        return false;

    }
}

bool semanticParsePRINT()
{
    logger.log("semanticParsePRINT");
    if (!tableCatalogue.isTable(parsedQuery.printRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    return true;
}

void executePRINT()
{
    logger.log("executePRINT");
    Table* table = tableCatalogue.getTable(parsedQuery.printRelationName);
    table->print();
    return;
}
