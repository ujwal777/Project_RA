#include"bufferManager.h"
/**
 * @brief The cursor is an important component of the system. To read from a
 * table, you need to initialize a cursor. The cursor reads rows from a page one
 * at a time.
 *
 */
class Cursor{
    public:
    Page page;
    int pageIndex;
    string tableName;
    int pagePointer;

    public:
    Cursor(string tableName, int pageIndex);
    vector<int> getNext();
    vector<int> getnextline(int j,int start,int end,int pageindex,vector<int>& res,int write);
    void transposeLine(int i);
    void nextPage(int pageIndex);
    void pointZero();
};