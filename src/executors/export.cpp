#include "global.h"

/**
 * @brief 
 * SYNTAX: EXPORT <relation_name> 
 */

bool syntacticParseEXPORT()
{
    logger.log("syntacticParseEXPORT");
    if ((tokenizedQuery.size() == 2 && tokenizedQuery[1]!="MATRIX") || (tokenizedQuery.size()==3 && tokenizedQuery[1]=="MATRIX")){
        parsedQuery.queryType = EXPORT;

        if(tokenizedQuery[1]=="MATRIX"){
            parsedQuery.ismatrix = true;
             parsedQuery.exportRelationName = tokenizedQuery[2];
        }
        else{
            parsedQuery.ismatrix = false;
            parsedQuery.exportRelationName = tokenizedQuery[1];
        }
        return true;
    }
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
}

bool semanticParseEXPORT()
{
    logger.log("semanticParseEXPORT");
    //Table should exist
    if (tableCatalogue.isTable(parsedQuery.exportRelationName))
        return true;
    cout << "SEMANTIC ERROR: No such relation exists" << endl;
    return false;
}

void executeEXPORT()
{
    logger.log("executeEXPORT");
    Table* table = tableCatalogue.getTable(parsedQuery.exportRelationName);
    table->makePermanent();
    return;
}