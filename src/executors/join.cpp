#include "global.h"

/**
 * @brief
 * SYNTAX: R <- JOIN USING <NESTED|PARTHASH> relation_name1, relation_name2 ON column_name1 bin_op column_name2 BUFFER buffer_size
 */
bool syntacticParseJOIN()
{
    logger.log("syntacticParseJOIN");
    if (tokenizedQuery.size() != 13 || tokenizedQuery[3] != "USING" || tokenizedQuery[7] != "ON" || tokenizedQuery[11] != "BUFFER")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = JOIN;
    parsedQuery.joinResultRelationName = tokenizedQuery[0];
    parsedQuery.joinFirstRelationName = tokenizedQuery[5];
    parsedQuery.joinSecondRelationName = tokenizedQuery[6];
    parsedQuery.joinFirstColumnName = tokenizedQuery[8];
    parsedQuery.joinSecondColumnName = tokenizedQuery[10];

    try
    {
        parsedQuery.joinBufferSize = stoi(tokenizedQuery[12]);
    }
    catch (...)
    {
        cout<< "SYNTAX ERROR" << endl;
        return false;
    }

    string joinType = tokenizedQuery[4];
    if (joinType == "NESTED")
        parsedQuery.joinType = NESTED;
    else if (joinType == "PARTHASH")
        parsedQuery.joinType = PARTHASH;
    else
    {
        cout<< "SYNTAX ERROR" << endl;
        return false;
    }

    string binaryOperator = tokenizedQuery[9];
    if (binaryOperator == "<")
        parsedQuery.joinBinaryOperator = LESS_THAN;
    else if (binaryOperator == ">")
        parsedQuery.joinBinaryOperator = GREATER_THAN;
    else if (binaryOperator == ">=" || binaryOperator == "=>")
        parsedQuery.joinBinaryOperator = GEQ;
    else if (binaryOperator == "<=" || binaryOperator == "=<")
        parsedQuery.joinBinaryOperator = LEQ;
    else if (binaryOperator == "==")
        parsedQuery.joinBinaryOperator = EQUAL;
    else if (binaryOperator == "!=")
        parsedQuery.joinBinaryOperator = NOT_EQUAL;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseJOIN()
{
    logger.log("semanticParseJOIN");

    if (tableCatalogue.isTable(parsedQuery.joinResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.joinFirstRelationName) || !tableCatalogue.isTable(parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.joinFirstColumnName, parsedQuery.joinFirstRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.joinSecondColumnName, parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    if(tableCatalogue.getTable(parsedQuery.joinResultRelationName)){
        cout<<"SEMANTIC ERROR: Table already Exists"<<endl;
        return false;
    }
    return true;
}

// Other than equal operator join
void joinOther(int parts, float ma, float mi)
{
    //<<ma<<" "<<mi<<" "<<parts<<endl;


    BLOCK_ACCESS=0;
    int hash = parsedQuery.joinBufferSize - 1;
    Table table1 = *tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
    Table table2 = *tableCatalogue.getTable(parsedQuery.joinSecondRelationName);
    bufferManager.clearBuffer(table1.tableName,0);
    vector<string> cols = table1.columns;
    for (auto it = table2.columns.begin(); it != table2.columns.end(); it++)
    {
        cols.push_back(*it);
    }
    Table *result = new Table(parsedQuery.joinResultRelationName, cols);

    int index1 = table1.getColumnIndex(parsedQuery.joinFirstColumnName);
    int index2 = table2.getColumnIndex(parsedQuery.joinSecondColumnName);

    vector<vector<vector<int>>> partitions(hash);
    vector<int> overflows(hash, 0);
    for (int i = 0; i < table1.blockCount; i++)
    {
        Page block = bufferManager.getPage(table1.tableName, i);
        vector<vector<int>> rows = block.getblock();
        for (int row = 0; row < rows.size(); row++)
        {
            int mpvalue =(int) (rows[row][index1] / parts)+ma/parts;
            //<<mpvalue<<" "<<parts<<endl;
            if (partitions[mpvalue].size() == table1.maxRowsPerBlock)
            {
                Page page = Page();
                page.writeBlock(partitions[mpvalue], mpvalue, table1.tableName, overflows[mpvalue], 0);
                overflows[mpvalue] += 1;
                partitions[mpvalue].clear();
            }
            partitions[mpvalue].push_back(rows[row]);
        }
    }
    for (int i = 0; i < partitions.size(); i++)
    {
        if (partitions[i].size() != 0)
        {
            Page page = Page();
            page.writeBlock(partitions[i], i, table1.tableName, overflows[i], 1);
        }
    }
    int ind = 0;
    for (auto it = partitions.begin(); it != partitions.end(); it++)
    {
        (*it).clear();
        overflows[ind] = 0;
        ind++;
    }
    bufferManager.clearBuffer(table1.tableName, 0);

    for (int i = 0; i < table2.blockCount; i++)
    {
        Page block = bufferManager.getPage(table2.tableName, i);
        vector<vector<int>> rows = block.getblock();
        for (int row = 0; row < rows.size(); row++)
        {
            int mpvalue = (rows[row][index2] / parts)+ma/parts;
            //<<mpvalue<<endl;
            
            if (partitions[mpvalue].size() == table2.maxRowsPerBlock)
            {
                Page page = Page();
                page.writeBlock(partitions[mpvalue], mpvalue, table2.tableName, overflows[mpvalue], 0);
                overflows[mpvalue] += 1;
                partitions[mpvalue].clear();
            }
            partitions[mpvalue].push_back(rows[row]);
        }
    }
    for (int i = 0; i < partitions.size(); i++)
    {
        if (partitions[i].size() != 0)
        {
            Page page = Page();
            page.writeBlock(partitions[i], i, table2.tableName, overflows[i], 1);
        }
    }
    bufferManager.clearBuffer(table2.tableName,0);

    vector<vector<int>> resblock;
    for (int i = 0; i <= hash; i++)
    {

        // << i << endl;
        int j = 0;
        bool while_condition = false;
        int inc=0;

        if (parsedQuery.joinBinaryOperator == GREATER_THAN || parsedQuery.joinBinaryOperator == GEQ)
        {
            j = i;
            while_condition = (j >= 0);
            inc=-1;
        }
        else if (parsedQuery.joinBinaryOperator == LESS_THAN || parsedQuery.joinBinaryOperator == LEQ)
        {
            j = i;
            while_condition = (j <= hash);
            inc =1;
        }
        else
        {
            j = 0;
            while_condition = (j <= hash);
            inc =1;
        }
        while (while_condition)
        {
            // //<<j<<"*********"<<endl;
            string overflow1 = table1.tableName;

            while (overflow1 != "END")
            {
                Page bucket1 = bufferManager.getPartition(overflow1, i);
                vector<vector<int>> b1 = bucket1.getblock();
                for (int p1 = 0; p1 < b1.size(); p1++)
                {

                    string overflow2 = table2.tableName;

                    while (overflow2 != "END")
                    {
                        Page bucket2 = bufferManager.getPartition(overflow2, j);
                        vector<vector<int>> b2 = bucket2.getblock();

                        for (int p2 = 0; p2 < b2.size(); p2++)
                        {
                            vector<int> temp(b1[p1]);
                            temp.reserve(b1[p1].size() + b2[p2].size());
                            temp.insert(temp.end(), b2[p2].begin(), b2[p2].end());
                            if (resblock.size() == result->maxRowsPerBlock)
                            {
                                result->writeRows(resblock);
                                resblock.clear();
                            }
                            bool statement = (b1[p1][index1] == b2[p2][index2] && parsedQuery.joinBinaryOperator == EQUAL) || (b1[p1][index1] > b2[p2][index2] && parsedQuery.joinBinaryOperator == GREATER_THAN) || (b1[p1][index1] < b2[p2][index2] && parsedQuery.joinBinaryOperator == LESS_THAN) || (b1[p1][index1] >= b2[p2][index2] && parsedQuery.joinBinaryOperator == GEQ) || (b1[p1][index1] <= b2[p2][index2] && parsedQuery.joinBinaryOperator == LEQ);
                            if (statement)
                            {
                                resblock.push_back(temp);
                            }
                        }
                        overflow2 = bucket2.overflow;
                    }
                }
                overflow1 = bucket1.overflow;
               
            }
            j+=inc;
            if(inc==1){
                if(j>hash){
                    break;
                }
            }
            if(inc==-1){
                if(j<0){
                    break;
                }
            }
        }
    }

    if(resblock.size()!=0){
        result->writeRows(resblock);
        resblock.clear();
    }
    cout<<BLOCK_ACCESS<<endl;
    tableCatalogue.insertTable(result);
    result->blockify();
    //<<BLOCK_ACCESS<<endl;
    
}

void executeJOIN()
{
    logger.log("executeJOIN");
    if (parsedQuery.joinType == NESTED)
    {

        Table table1 = *tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
        Table table2 = *tableCatalogue.getTable(parsedQuery.joinSecondRelationName);

        vector<string> cols = table1.columns;
        for (auto it = table2.columns.begin(); it != table2.columns.end(); it++)
        {
            cols.push_back(*it);
        }
        int flag = 0;
        if (table1.rowCount > table2.rowCount)
        {
            table1 = *tableCatalogue.getTable(parsedQuery.joinSecondRelationName);
            table2 = *tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
            string tempname = parsedQuery.joinFirstColumnName;
            parsedQuery.joinFirstColumnName=parsedQuery.joinSecondColumnName;
            parsedQuery.joinSecondColumnName=tempname;
            parsedQuery.joinFirstRelationName = table1.tableName;
            parsedQuery.joinSecondRelationName = table2.tableName;
            flag = 1;
        }
        // // << table1.tableName << " " << table2.tableName << " " << parsedQuery.joinFirstColumnName << " " << parsedQuery.joinSecondColumnName << endl;
        bufferManager.clearBuffer(table1.tableName, 0);
        Table *result = new Table(parsedQuery.joinResultRelationName, cols);
        // Cursor cursor1= table1.getCursor();
        // Cursor cursor2= table2.getCursor();
        vector<int> row;
        vector<int> row2;
        if (parsedQuery.joinBufferSize != -1)
        {
            BLOCK_COUNT = parsedQuery.joinBufferSize;
        }
        CURR_BLOCK = 1;
        result_block_index = min(BLOCK_COUNT - 2, table1.blockCount);
        Cursor cursor1(table1.tableName, 0);
        int pageindex = 0;
        while (pageindex < min(BLOCK_COUNT - 2, table1.blockCount))
        {

            cursor1.nextPage(pageindex);
            pageindex += 1;
        }

        vector<int> initrow(table1.columnCount + table2.columnCount, -1);
        int respage = 0;
        int num1 = 0;
        int num2 = 0;
        vector<vector<int>> resblock;
        int count1 = 0;
        while (num1 < table1.blockCount)
        {
            num2 = 0;

            while (num2 < table2.blockCount)
            {
                // << num1 << " " << num2 << endl;
                int ind1 = 0;
                int index1 = table1.getColumnIndex(parsedQuery.joinFirstColumnName);
                int numrows2 = row2.size();
                int ind2 = 0;
                int index2 = table2.getColumnIndex(parsedQuery.joinSecondColumnName);
                // //<<table1.blockCount<<endl;
                // //<<table2.blockCount<<endl;
                CURR_BLOCK = 1;
                Cursor cursor1(table1.tableName, num1);
                Page block1 = bufferManager.getPage(table1.tableName, num1);
                vector<vector<int>> rows1 = block1.getblock();
                // row= cursor1.getNext();

                int numrows1 = row.size();
                while (ind1 < table1.rowsPerBlockCount[num1] && rows1[ind1].size() > 0)
                {
                    CURR_BLOCK = 3;
                    Cursor cursor2(table2.tableName, num2);
                    // row2=cursor2.getNext();
                    Page block2 = bufferManager.getPage(table2.tableName, num2);
                    vector<vector<int>> rows2 = block2.getblock();
                    ind2 = 0;

                    while (ind2 < table2.rowsPerBlockCount[num2] && rows2[ind2].size() > 0)
                    {
                        bool statement = (rows1[ind1][index1] == rows2[ind2][index2] && parsedQuery.joinBinaryOperator == EQUAL) || (rows1[ind1][index1] > rows2[ind2][index2] && parsedQuery.joinBinaryOperator == GREATER_THAN) || (rows1[ind1][index1] < rows2[ind2][index2] && parsedQuery.joinBinaryOperator == LESS_THAN) || (rows1[ind1][index1] >= rows2[ind2][index2] && parsedQuery.joinBinaryOperator == GEQ) || (rows1[ind1][index1] <= rows2[ind2][index2] && parsedQuery.joinBinaryOperator == LEQ);
                        if (flag == 1)
                        {
                            statement = (rows1[ind1][index1] == rows2[ind2][index2] && parsedQuery.joinBinaryOperator == EQUAL) || (rows1[ind1][index1] < rows2[ind2][index2] && parsedQuery.joinBinaryOperator == GREATER_THAN) || (rows1[ind1][index1] > rows2[ind2][index2] && parsedQuery.joinBinaryOperator == LESS_THAN) || (rows1[ind1][index1] <= rows2[ind2][index2] && parsedQuery.joinBinaryOperator == GEQ) || (rows1[ind1][index1] >= rows2[ind2][index2] && parsedQuery.joinBinaryOperator == LEQ);
                        }
                        if (statement)
                        {
                            if (flag == 0)
                            {
                                vector<int> temp(rows1[ind1]);
                                temp.reserve(rows1[ind1].size() + rows2[ind2].size());
                                temp.insert(temp.end(), rows2[ind2].begin(), rows2[ind2].end());
                                resblock.push_back(temp);
                            }
                            else
                            {
                                vector<int> temp(rows2[ind2]);
                                temp.reserve(rows1[ind1].size() + rows2[ind2].size());
                                temp.insert(temp.end(), rows1[ind1].begin(), rows1[ind1].end());
                                resblock.push_back(temp);
                            }

                            if (resblock.size() == result->maxRowsPerBlock)
                            {
                                // //<<result->maxRowsPerBlock<<endl;
                                result->writeRows(resblock);
                                respage += 1;
                                resblock.clear();
                            }
                            count1 += 1;
                        }
                        ind2 += 1;
                        // if(ind2<table2.rowsPerBlockCount[num2]){
                        // CURR_BLOCK=3;
                        //     // row2=cursor2.getNext();
                        // }
                    }
                    ind1 += 1;
                    // if(ind1 < table1.rowsPerBlockCount[num1]){
                    //     CURR_BLOCK=1;
                    //     row= cursor1.getNext();
                    // }
                }
                num2 += 1;
            }
            num1 += 1;
        }
        if (resblock.size() != 0)
        {
            result->writeRows(resblock);
            respage += 1;
            resblock.clear();
        }
        // // << count1 << endl;
        // cout<< BLOCK_ACCESS << endl;
        tableCatalogue.insertTable(result);
        result->blockify();
    }
    else
    {

        int hash = parsedQuery.joinBufferSize - 1;
        Table table1 = *tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
        Table table2 = *tableCatalogue.getTable(parsedQuery.joinSecondRelationName);
        vector<string> cols = table1.columns;
        for (auto it = table2.columns.begin(); it != table2.columns.end(); it++)
        {
            cols.push_back(*it);
        }
        // Table *result = new Table(parsedQuery.joinResultRelationName, cols);

        int index1 = table1.getColumnIndex(parsedQuery.joinFirstColumnName);
        int index2 = table2.getColumnIndex(parsedQuery.joinSecondColumnName);
        // //<<index1<<" "<<index2<<endl;

        if (parsedQuery.joinBinaryOperator != EQUAL)
        {
            float ma = INT_MIN;
            float mi = INT_MAX;
            for (int i = 0; i < table1.blockCount; i++)
            {
                Page block = bufferManager.getPage(table1.tableName, i);
                vector<vector<int>> rows = block.getblock();
                for (int j = 0; j < rows.size(); j++)
                {
                    ma = max((float)rows[j][index1], ma);
                    mi = min((float)rows[j][index1], mi);
                }
            }

            for (int i = 0; i < table2.blockCount; i++)
            {
                Page block = bufferManager.getPage(table2.tableName, i);
                vector<vector<int>> rows = block.getblock();
                for (int j = 0; j < rows.size(); j++)
                {
                    ma = max((float)rows[j][index1], ma);
                    mi = min((float)rows[j][index1], mi);
                }
            }
            int parts = ceil((float)(ma - mi) / hash);
            joinOther(parts, ma, mi);
            return;
        }
        Table *result = new Table(parsedQuery.joinResultRelationName, cols);

        BLOCK_ACCESS = 0;
        // //<<"**************"<<endl;

        int pindex = 0;
        vector<vector<vector<int>>> partitions(hash);
        vector<int> overflows(hash, 0);
        //  //<<"**************"<<endl;
        for (int i = 0; i < table1.blockCount; i++)
        {
            Page block = bufferManager.getPage(table1.tableName, i);
            // //<<"************** "<<i<<endl;
            vector<vector<int>> rows = block.getblock();
            for (int row = 0; row < rows.size(); row++)
            {
                int mpvalue = ((rows[row][index1] % hash)+hash)%hash;
                // if(mpvalue<0){
                //     mpvalue=abs(rows[row][index1]%(-1*hash));
                    
                // }
            //    //<<rows[row][index2]<<" "<<mpvalue<<endl;

                if (partitions[mpvalue].size() == table1.maxRowsPerBlock)
                {
                    Page page = Page();
                    page.writeBlock(partitions[mpvalue], mpvalue, table1.tableName, overflows[mpvalue], 0);
                    overflows[mpvalue] += 1;
                    partitions[mpvalue].clear();
                }
                partitions[mpvalue].push_back(rows[row]);
            }
        }
        // //<<"**************"<<endl;
        for (int i = 0; i < partitions.size(); i++)
        {
            if (partitions[i].size() != 0)
            {
                Page page = Page();
             
                page.writeBlock(partitions[i], i, table1.tableName, overflows[i], 1);
            }
        }
        int ind = 0;
        for (auto it = partitions.begin(); it != partitions.end(); it++)
        {
            (*it).clear();
            overflows[ind] = 0;
            ind++;
        }
        bufferManager.clearBuffer(table1.tableName, 0);

        for (int i = 0; i < table2.blockCount; i++)
        {
            Page block = bufferManager.getPage(table2.tableName, i);
            vector<vector<int>> rows = block.getblock();
            for (int row = 0; row < rows.size(); row++)
            {
                // //<<rows[row][index2]<<" "<<index2<<endl;

                int mpvalue = ((rows[row][index2] % hash) + hash)%hash;
                //  int mpvalue = rows[row][index1] % hash;
               
                if (partitions[mpvalue].size() == table2.maxRowsPerBlock)
                {
                    Page page = Page();
                    page.writeBlock(partitions[mpvalue], mpvalue, table2.tableName, overflows[mpvalue], 0);
                    overflows[mpvalue] += 1;
                    partitions[mpvalue].clear();
                }
                partitions[mpvalue].push_back(rows[row]);
            }
        }
        for (int i = 0; i < partitions.size(); i++)
        {
            if (partitions[i].size() != 0)
            {
                Page page = Page();
                page.writeBlock(partitions[i], i, table2.tableName, overflows[i], 1);
            }
        }
        bufferManager.clearBuffer(table2.tableName, 0);
        
        //<<"**************"<<endl;

        vector<vector<int>> resblock;
        for (int i = 0; i <= hash; i++)
        {
            string overflow1 = table1.tableName;

            // << i << endl;
            while (overflow1 != "END")
            {
                Page bucket1 = bufferManager.getPartition(overflow1, i);
                vector<vector<int>> b1 = bucket1.getblock();
                for (int p1 = 0; p1 < b1.size(); p1++)
                {
                    string overflow2 = table2.tableName;

                    while (overflow2 != "END")
                    {
                        Page bucket2 = bufferManager.getPartition(overflow2, i);
                        vector<vector<int>> b2 = bucket2.getblock();
                        for (int p2 = 0; p2 < b2.size(); p2++)
                        {
                            vector<int> temp(b1[p1]);
                            temp.reserve(b1[p1].size() + b2[p2].size());
                            temp.insert(temp.end(), b2[p2].begin(), b2[p2].end());
                            if (resblock.size() == result->maxRowsPerBlock)
                            {
                                result->writeRows(resblock);
                                resblock.clear();
                            }
                            // bool statement = (rows1[ind1][index1] == rows2[ind2][index2] && parsedQuery.joinBinaryOperator == EQUAL) || (rows1[ind1][index1] > rows2[ind2][index2] && parsedQuery.joinBinaryOperator == GREATER_THAN) || (rows1[ind1][index1] < rows2[ind2][index2] && parsedQuery.joinBinaryOperator == LESS_THAN) || (rows1[ind1][index1] >= rows2[ind2][index2] && parsedQuery.joinBinaryOperator == GEQ) || (rows1[ind1][index1] <= rows2[ind2][index2] && parsedQuery.joinBinaryOperator == LEQ);
                            if (b1[p1][index1] == b2[p2][index2])
                            {
                                resblock.push_back(temp);
                            }
                        }
                        overflow2 = bucket2.overflow;
                    }
                }
                overflow1 = bucket1.overflow;
            }
        }
        if (resblock.size() != 0)
        {
            result->writeRows(resblock);
            resblock.clear();
        }
        cout<< BLOCK_ACCESS << endl;
        
        tableCatalogue.insertTable(result);
        result->blockify();
    }
    
   
    return;
}
