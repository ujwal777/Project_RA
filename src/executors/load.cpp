#include "global.h"
/**
 * @brief 
 * SYNTAX: LOAD relation_name
 */
bool syntacticParseLOAD()
{
    logger.log("syntacticParseLOAD");
    if ((tokenizedQuery.size() == 2 && tokenizedQuery[1]!="MATRIX") || (tokenizedQuery.size()==3 && tokenizedQuery[1]=="MATRIX")){
        parsedQuery.queryType = LOAD;
        if(tokenizedQuery[1]=="MATRIX"){
            parsedQuery.ismatrix=true;
             parsedQuery.loadRelationName = tokenizedQuery[2];


        }
        else{
            parsedQuery.ismatrix=false;
            parsedQuery.loadRelationName = tokenizedQuery[1];
        }
        return true;
    }
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
}

bool semanticParseLOAD()
{
    logger.log("semanticParseLOAD");
    if (tableCatalogue.isTable(parsedQuery.loadRelationName))
    {
        cout << "SEMANTIC ERROR: Relation already exists" << endl;
        return false;
    }

    if (!isFileExists(parsedQuery.loadRelationName))
    {
        cout << "SEMANTIC ERROR: Data file doesn't exist" << endl;
        return false;
    }

    return true;
}

void executeLOAD()
{
    logger.log("executeLOAD");

    Table *table = new Table(parsedQuery.loadRelationName);
    if (table->load())
    {
        tableCatalogue.insertTable(table);
        cout << "Loaded Table. Column Count: " << table->columnCount << " Row Count: " << table->rowCount << endl;
    }

    return;
}