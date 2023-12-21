#include "global.h"

/**
 * @brief Construct a new Table:: Table object
 *
 */
Table::Table()
{
    logger.log("Table::Table");
}

/**
 * @brief Construct a new Table:: Table object used in the case where the data
 * file is available and LOAD command has been called. This command should be
 * followed by calling the load function;
 *
 * @param tableName
 */
Table::Table(string tableName)
{
    logger.log("Table::Table");
    this->sourceFileName = "../data/" + tableName + ".csv";
    this->tableName = tableName;
}

/**
 * @brief Construct a new Table:: Table object used when an assignment command
 * is encountered. To create the table object both the table name and the
 * columns the table holds should be specified.
 *
 * @param tableName
 * @param columns
 */
Table::Table(string tableName, vector<string> columns)
{
    logger.log("Table::Table");
    this->sourceFileName = "../data/temp/" + tableName + ".csv";
    this->tableName = tableName;
    this->columns = columns;
    this->start=0;
    this->columnCount = columns.size();
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * columnCount));
    this->writeRow<string>(columns);
}

/**
 * @brief The load function is used when the LOAD command is encountered. It
 * reads data from the source file, splits it into blocks and updates table
 * statistics.
 *
 * @return true if the table has been successfully loaded
 * @return false if an error occurred
 */
bool Table::load()
{
    logger.log("Table::load");
    fstream fin(this->sourceFileName, ios::in);
    string line;
    if(parsedQuery.ismatrix){
        this->ismatrix=true;
    }
    // cout<<"extracting column names for matrix";
    if (getline(fin, line))
    {
        fin.close();
       

        if (this->extractColumnNames(line))
        {
                
            if (this->blockify()){

                return true;
            }
        }
    }
    fin.close();
    return false;
}

/**
 * @brief Function extracts column names from the header line of the .csv data
 * file.
 *
 * @param line
 * @return true if column names successfully extracted (i.e. no column name
 * repeats)
 * @return false otherwise
 */
bool Table::extractColumnNames(string firstLine)
{
    logger.log("Table::extractColumnNames");
    unordered_set<string> columnNames;
    string word;
    stringstream s(firstLine);
    while (getline(s, word, ','))
    {
        word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
        if (columnNames.count(word) && !(parsedQuery.ismatrix))
            return false;
        columnNames.insert(word);

        this->columns.emplace_back(word);
    }
    this->columnCount = this->columns.size();
    if (parsedQuery.ismatrix)
    {
        this->maxElementsperblock = (uint)((BLOCK_SIZE * 1000)) / (sizeof(int) * (uint)1);
    }
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->columnCount));
    return true;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size.
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Table::blockify()
{
    if (parsedQuery.ismatrix)
    {
        logger.log("MATRIX::blockify");
    }
    else
    {
        logger.log("Table::blockify");
    }
    ifstream fin(this->sourceFileName, ios::in);
    string line, word;
    vector<int> row(this->columnCount, 0);
    vector<vector<int>> rowsInPage(this->maxRowsPerBlock, row);
    int pageCounter = 0;
    unordered_set<int> dummy;
    dummy.clear();
    if (!(parsedQuery.ismatrix))
    {
        this->distinctValuesInColumns.assign(this->columnCount, dummy);
        this->distinctValuesPerColumnCount.assign(this->columnCount, 0);
    }
    if (!(parsedQuery.ismatrix))
    {
        getline(fin, line);
        while (getline(fin, line))
        {
            stringstream s(line);
            for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
            {
                if (!getline(s, word, ','))
                    return false;
                row[columnCounter] = stoi(word);
                rowsInPage[pageCounter][columnCounter] = row[columnCounter];
            }
            pageCounter++;
            this->updateStatistics(row);
            if (pageCounter == this->maxRowsPerBlock)
            {
                bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
                this->blockCount++;
                this->rowsPerBlockCount.emplace_back(pageCounter);
                pageCounter = 0;
            }
        }
        if (pageCounter)
        {
            bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
            this->blockCount++;
            this->rowsPerBlockCount.emplace_back(pageCounter);
            pageCounter = 0;
        }

        if (this->rowCount == 0){
             return false;
        }
           
        this->distinctValuesInColumns.clear();
    }

    // updated blockify for matrix
    else
    {
        vector<int> row(maxElementsperblock,0);
        vector<vector<int>> rowsInPage(1, row);
        // return false;
        int curr = 0;
        int ind = 0;
        vector<int> sep;
        this->blockCount=0;
        while (getline(fin, line))
        {
            stringstream s(line);
            for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
            {
                if (!getline(s, word, ','))
                    return false;
                row[ind] = stoi(word);
                rowsInPage[0][ind] = row[ind];
                ind++;
                if (ind == maxElementsperblock)
                {
                    bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter,sep,this->start);
                    this->start=sep.size();
                    ind = 0;
                    pageCounter = 0;
                    this->blockCount++;
                }
            }
            this->sep.push_back(ind);

            sep.push_back(ind);
            pageCounter++;
            this->updateStatistics(row);
        }
        sep.push_back(-1);

        if (ind)
        {
            bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter,sep,this->start);
            ind = 0;
            pageCounter = 0;
            this->blockCount++;
        }

        if (this->rowCount == 0){
            return false;
        }
        
        // return false;
        // this->distinctValuesInColumns.clear();
    }
    return true;
}

/**
 * @brief Given a row of values, this function will update the statistics it
 * stores i.e. it updates the number of rows that are present in the column and
 * the number of distinct values present in each column. These statistics are to
 * be used during optimisation.
 *
 * @param row
 */
void Table::updateStatistics(vector<int> row)
{
    this->rowCount++;
    if(parsedQuery.ismatrix){
        return;
    }
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (!this->distinctValuesInColumns[columnCounter].count(row[columnCounter]))
        {
            this->distinctValuesInColumns[columnCounter].insert(row[columnCounter]);
            this->distinctValuesPerColumnCount[columnCounter]++;
        }
    }
}

/**
 * @brief Checks if the given column is present in this table.
 *
 * @param columnName
 * @return true
 * @return false
 */
bool Table::isColumn(string columnName)
{
    logger.log("Table::isColumn");
    for (auto col : this->columns)
    {
        if (col == columnName)
        {
            return true;
        }
    }
    return false;
}

/**
 * @brief Renames the column indicated by fromColumnName to toColumnName. It is
 * assumed that checks such as the existence of fromColumnName and the non prior
 * existence of toColumnName are done.
 *
 * @param fromColumnName
 * @param toColumnName
 */
void Table::renameColumn(string fromColumnName, string toColumnName)
{
    logger.log("Table::renameColumn");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (columns[columnCounter] == fromColumnName)
        {
            columns[columnCounter] = toColumnName;
            break;
        }
    }
    return;
}

/**
 * @brief Function prints the first few rows of the table. If the table contains
 * more rows than PRINT_COUNT, exactly PRINT_COUNT rows are printed, else all
 * the rows are printed.
 *
 */
void Table::print()
{
    logger.log("Table::print");
    uint count = min((long long)20, this->rowCount);

    // // print headings
    bufferManager.clearBuffer(this->tableName,0);
    Cursor cursor(this->tableName, 0);
    vector<int> row;
    if(parsedQuery.ismatrix){
        for (int rowCounter = 0; rowCounter < count; rowCounter++)
        {
            row = cursor.getNext();
            this->writeRow(row, cout);
        }
    }
    else{
        this->writeRow(this->columns, cout);
        for (int rowCounter = 0; rowCounter < count; rowCounter++)
        {
            row = cursor.getNext();
            this->writeRow(row, cout);
        }
    }
    printRowCount(this->rowCount);
}

/**
 * @brief This function returns one row of the table using the cursor object. It
 * returns an empty row is all rows have been read.
 *
 * @param cursor
 * @return vector<int>
 */
void Table::getNextPage(Cursor *cursor)
{
    logger.log("Table::getNext");

    if (cursor->pageIndex < this->blockCount - 1)
    {
        cursor->nextPage(cursor->pageIndex + 1);
    }
}

/**
 * @brief called when EXPORT command is invoked to move source file to "data"
 * folder.
 *
 */
void Table::makePermanent()
{
    logger.log("Table::makePermanent");    
    if (!this->isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
    string newSourceFile = "../data/" + this->tableName + ".csv";
    ofstream fout(newSourceFile, ios::out);


    if(!parsedQuery.ismatrix) // print headings
        this->writeRow(this->columns, fout);

    Cursor cursor(this->tableName, 0);
    vector<int> row;
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        row = cursor.getNext();
        // cout<<row.size()<<"********"<<endl;
        this->writeRow(row, fout);
    }
    fout.close();
}

/**
 * @brief Function to check if table is already exported
 *
 * @return true if exported
 * @return false otherwise
 */
bool Table::isPermanent()
{
    logger.log("Table::isPermanent");
    if (this->sourceFileName == "../data/" + this->tableName + ".csv")
        return true;
    return false;
}

/**
 * @brief The unload function removes the table from the database by deleting
 * all temporary files created as part of this table
 *
 */
void Table::unload()
{
    logger.log("Table::~unload");
    for (int pageCounter = 0; pageCounter < this->blockCount; pageCounter++)
        bufferManager.deleteFile(this->tableName, pageCounter);
    if (!isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
}

/**
 * @brief Function that returns a cursor that reads rows from this table
 *
 * @return Cursor
 */
Cursor Table::getCursor()
{
    logger.log("Table::getCursor");
    Cursor cursor(this->tableName, 0);
    return cursor;
}
/**
 * @brief Function that returns the index of column indicated by columnName
 *
 * @param columnName
 * @return int
 */
int Table::getColumnIndex(string columnName)
{
    logger.log("Table::getColumnIndex");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (this->columns[columnCounter] == columnName)
        
            return columnCounter;
    }
}
