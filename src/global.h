#include"executor.h"

extern float BLOCK_SIZE;
extern uint BLOCK_COUNT;
extern float CURR_BLOCK;
extern uint BLOCK_ACCESS;
extern uint PRINT_COUNT;
extern uint result_block_index;
extern vector<string> tokenizedQuery;
extern ParsedQuery parsedQuery;
extern TableCatalogue tableCatalogue;
extern BufferManager bufferManager;