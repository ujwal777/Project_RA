#include "global.h"

/**
 * @brief File contains method to process GROUP BY commands.
 *
 * syntax:
 * <new_table> <- GROUP BY <grouping_attribute> FROM <table_name> RETURN MAX|MIN|SUM|AVG(<attribute>)
 *
 */
bool syntacticParseGROUPBY()
{

    logger.log("syntacticParseGROUPBY");

    if (tokenizedQuery.size() != 9 || tokenizedQuery[3] != "BY" || tokenizedQuery[5] != "FROM" || tokenizedQuery[7] != "RETURN" || tokenizedQuery[8][3] != '(' || tokenizedQuery[8][tokenizedQuery[8].size() - 1] != ')')
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = GROUPBY;

    parsedQuery.groupbyResultRelationName = tokenizedQuery[0];
    parsedQuery.groupbyColumnName = tokenizedQuery[4];
    parsedQuery.groupbyRelationName = tokenizedQuery[6];

    string aggregateOperator = tokenizedQuery[8].substr(0, 3);

    if(aggregateOperator == "MIN") 
        parsedQuery.aggregateOperator = MIN;
    else if (aggregateOperator == "MAX")
        parsedQuery.aggregateOperator = MAX;
    else if (aggregateOperator == "SUM")
        parsedQuery.aggregateOperator = SUM;
    else if (aggregateOperator == "AVG")
        parsedQuery.aggregateOperator = AVG;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.groupbyAggregateColumnName = tokenizedQuery[8].substr(4, tokenizedQuery[8].size() - 5);
    return true;
}

bool semanticParseGROUPBY()
{
    logger.log("semanticParseGROUPBY");

    if (tableCatalogue.isTable(parsedQuery.groupbyResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.groupbyRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupbyColumnName, parsedQuery.groupbyRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupbyAggregateColumnName, parsedQuery.groupbyRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    return true;
}

void executeGROUPBY()
{
    logger.log("executeGROUPBY");

    Table table = *(tableCatalogue.getTable(parsedQuery.groupbyRelationName));

    vector<string> columns;
    columns.emplace_back(parsedQuery.groupbyColumnName);

    string columnString = parsedQuery.groupbyAggregateColumnName;
    if(parsedQuery.aggregateOperator == MIN) 
        columnString = "MIN" + columnString;
    else if (parsedQuery.aggregateOperator == MAX)
        columnString = "MAX" + columnString;
    else if (parsedQuery.aggregateOperator == SUM)
        columnString = "SUM" + columnString;
    else if (parsedQuery.aggregateOperator == AVG)
        columnString = "AVG" + columnString;

    columns.emplace_back(columnString);

    Table *resultantTable = new Table(parsedQuery.groupbyResultRelationName, columns);

    int columnIndex = table.getColumnIndex(parsedQuery.groupbyColumnName);
    int aggregateColumnIndex = table.getColumnIndex(parsedQuery.groupbyAggregateColumnName);
    int distinctValueCount = table.distinctValuesPerColumnCount[columnIndex];   

    // Key: Distinct value in the grouping column
    // Pair: <Aggregate value, Count of values>
    unordered_map<int, pair<int, int>> result;
    vector<int> key_order; // To store the keys in the order they exist in the relation

    Cursor cursor = table.getCursor();

    vector<int> row = cursor.getNext();
    
    while (!row.empty())
    {
        int key = row[columnIndex];

        if (result.find(key) == result.end()) {
            // Key doesn't exist
            result[key] = { row[aggregateColumnIndex], 1 };
            key_order.push_back(key);
        }
        else {
            // Key exists
            if (parsedQuery.aggregateOperator == MIN) {
                if (row[aggregateColumnIndex] < result[key].first) {
                    result[key] = { row[aggregateColumnIndex], 1 };
                }
            }
            else if (parsedQuery.aggregateOperator == MAX) {
                if (row[aggregateColumnIndex] > result[key].first) {
                    result[key] = { row[aggregateColumnIndex], 1 };
                }
            }
            else if (parsedQuery.aggregateOperator == SUM || parsedQuery.aggregateOperator == AVG) {
                result[key].first += row[aggregateColumnIndex];
                result[key].second++;
            }
        }

        row = cursor.getNext();
    }

    for (auto it = key_order.begin(); it != key_order.end(); it++) {

        vector<int> resultantRow;
        resultantRow.reserve(resultantTable->columnCount);

        pair<int, int> value = result[*it];

        resultantRow.emplace_back(*it);
        
        if (parsedQuery.aggregateOperator == AVG) {
          resultantRow.emplace_back(round(value.first * 1.0 / value.second));
        }
        else {
            resultantRow.emplace_back(value.first);
        }

        resultantTable->writeRow<int>(resultantRow);
    }

    resultantTable->blockify();

    cout << BLOCK_ACCESS << endl;

    tableCatalogue.insertTable(resultantTable);
    return;
}