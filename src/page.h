#include"logger.h"
/**
 * @brief The Page object is the main memory representation of a physical page
 * (equivalent to a block). The page class and the page.h header file are at the
 * bottom of the dependency tree when compiling files. 
 *<p>
 * Do NOT modify the Page class. If you find that modifications
 * are necessary, you may do so by posting the change you want to make on Moodle
 * or Teams with justification and gaining approval from the TAs. 
 *</p>
 */

class Page{

    string tableName;
    int pageIndex;
    int columnCount;
    int rowCount;
    int start;
   
    
    vector<vector<int>> rows;

    public:
    vector<int> sep;
    string overflow;

    string pageName = "";
    Page();
    Page(string tablename);
    Page(string tableName, int pageIndex);
    Page(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount);
    Page(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount, vector<int> sep,int start);
    vector<int> getRow(int rowIndex);
    int getRowElement(int rowIndex,int j,vector<int> sep);
    vector<int> getRow(int rowIndex, vector<int> sep,vector<int> & result);
    void getelementsRange(int row,int start,int end,vector<int>& res);
    void putRowElement(int row,int j,vector<int>& res);
    void writePage();
    void updatePage(vector<int> &res, vector<int> sep);
    void writePage(int row,vector<int> sep);
    void writeBlock(vector<vector<int> >,int partition_no,string tablename,int overflow,int islast);
    vector<vector<int> > getblock();
};