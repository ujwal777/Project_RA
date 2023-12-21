#include "global.h"

BufferManager::BufferManager()
{
    logger.log("BufferManager::BufferManager");
}

/**
 * @brief Function called to read a page from the buffer manager. If the page is
 * not present in the pool, the page is read and then inserted into the pool.
 *
 * @param tableName
 * @param pageIndex
 * @return Page
 */
Page BufferManager::getPage(string tableName, int pageIndex)
{
    logger.log("BufferManager::getPage");
    string pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);

    if (this->inPool(pageName))
    {
        return this->getFromPool(pageName);
    }
    else
    {
        BLOCK_ACCESS += 1;
        // cout << tableName << " " << pageIndex << endl;
        return this->insertIntoPool(tableName, pageIndex);
    }
}

Page BufferManager::getPartition(string tableName, int pageIndex)
{
    logger.log("BufferManager::getPartition");
    string pageName = "../data/temp/" + tableName + "_partition_" + to_string(pageIndex);

    if (this->inPool(pageName))
    {
        return this->getFromPool(pageName);
    }
    else
    {
        BLOCK_ACCESS += 1;
        // cout << tableName << " " << pageIndex << endl;
        return this->insertIntoPool(pageName, -1);
    }
}

/**
 * @brief Checks to see if a page exists in the pool
 *
 * @param pageName
 * @return true
 * @return false
 */
bool BufferManager::inPool(string pageName)
{
    logger.log("BufferManager::inPool");
    for (auto page : this->pages)
    {
        if (pageName == page.pageName)
            return true;
    }
    return false;
}

/**
 * @brief If the page is present in the pool, then this function returns the
 * page. Note that this function will fail if the page is not present in the
 * pool.
 *
 * @param pageName
 * @return Page
 */
Page BufferManager::getFromPool(string pageName)
{
    logger.log("BufferManager::getFromPool");
    for (auto page : this->pages)
        if (pageName == page.pageName)
            return page;
}

/**
 * @brief Inserts page indicated by tableName and pageIndex into pool. If the
 * pool is full, the pool ejects the oldest inserted page from the pool and adds
 * the current page at the end. It naturally follows a queue data structure.
 *
 * @param tableName
 * @param pageIndex
 * @return Page
 */
Page BufferManager::insertIntoPool(string tableName, int pageIndex)
{
    logger.log("BufferManager::insertIntoPool");
    // cout<<CURR_BLOCK<<endl;
    // Page page;
    if (pageIndex == -1)
    {
       
        Page page(tableName);
        if (parsedQuery.queryType != JOIN || parsedQuery.joinType == PARTHASH)
        {
            if (this->pages.size() >= BLOCK_COUNT)
                pages.pop_front();
            pages.push_back(page);
        }
        else
        {
            if (CURR_BLOCK == 3)
            {
                if (this->pages.size() >= BLOCK_COUNT - 1)
                    pages.pop_back();
                pages.push_back(page);
            }
            else
            {
                if (this->pages.size() >= BLOCK_COUNT - 1)
                {

                    pages.erase(pages.begin() + BLOCK_COUNT - 3);
                    pages.insert(pages.begin(), page);
                }
                else
                {
                    pages.insert(pages.begin(), page);
                }
            }
        }
        // cout<<"hello endl"<<endl;

        return page;
    }
    else
    {
        Page page(tableName, pageIndex);

        if (parsedQuery.queryType != JOIN || parsedQuery.joinType == PARTHASH)
        {
            if (this->pages.size() >= BLOCK_COUNT)
                pages.pop_front();
            pages.push_back(page);
        }
        else
        {
            if (CURR_BLOCK == 3)
            {
                if (this->pages.size() >= BLOCK_COUNT - 1)
                    pages.pop_back();
                pages.push_back(page);
            }
            else
            {
                if (this->pages.size() >= BLOCK_COUNT - 1)
                {

                    pages.erase(pages.begin() + BLOCK_COUNT - 3);
                    pages.insert(pages.begin(), page);
                }
                else
                {
                    pages.insert(pages.begin(), page);
                }
            }
        }

        return page;
    }
}

void BufferManager::clearBuffer(string tableName, int pageIndex)
{
    logger.log("BufferManager::insertIntoPool");
    Page page(tableName, pageIndex);
    while (!pages.empty())
    {
        pages.pop_front();
    }
    // pages.push_back(page);
    // return page;
}

/**
 * @brief The buffer manager is also responsible for writing pages. This is
 * called when new tables are created using assignment statements.
 *
 * @param tableName
 * @param pageIndex
 * @param rows
 * @param rowCount
 */
void BufferManager::writePage(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount)
{
    logger.log("BufferManager::writePage");
    Page page(tableName, pageIndex, rows, rowCount);
    page.writePage();
}

void BufferManager::writePage(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount, vector<int> sep, int start)
{
    logger.log("BufferManager::writePage");
    Page page(tableName, pageIndex, rows, rowCount, sep, start);
    page.writePage();
}

/**
 * @brief Deletes file names fileName
 *
 * @param fileName
 */
void BufferManager::deleteFile(string fileName)
{

    if (remove(fileName.c_str()))
        logger.log("BufferManager::deleteFile: Err");
    else
        logger.log("BufferManager::deleteFile: Success");
}

/**
 * @brief Overloaded function that calls deleteFile(fileName) by constructing
 * the fileName from the tableName and pageIndex.
 *
 * @param tableName
 * @param pageIndex
 */
void BufferManager::deleteFile(string tableName, int pageIndex)
{
    logger.log("BufferManager::deleteFile");
    string fileName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    this->deleteFile(fileName);
}