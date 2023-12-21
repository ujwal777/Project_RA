//Server Code
#include "global.h"
#include <unistd.h>
#include <signal.h>

using namespace std;

float BLOCK_SIZE = 8;
float CURR_BLOCK=3;
uint BLOCK_COUNT = 2;
uint BLOCK_ACCESS=0;
uint result_block_index=0;
uint PRINT_COUNT = 20;
Logger logger;
vector<string> tokenizedQuery;
ParsedQuery parsedQuery;
TableCatalogue tableCatalogue;
BufferManager bufferManager;

void doCommand()
{
    logger.log("doCommand");
    if (syntacticParse() && semanticParse())
        executeCommand();
    return;
}

int main(void)
{

    regex delim("[^\\s,]+");
    string command;
    system("rm -rf ../data/temp");
    system("mkdir ../data/temp");

    while(!cin.eof())
    {
        CURR_BLOCK=3;
        cout << "\n> ";
        tokenizedQuery.clear();
        parsedQuery.clear();
        logger.log("\nReading New Command: ");
        getline(cin, command);
        logger.log(command);
        BLOCK_ACCESS=0;

        auto words_begin = std::sregex_iterator(command.begin(), command.end(), delim);
        auto words_end = std::sregex_iterator();
        for (std::sregex_iterator i = words_begin; i != words_end; ++i)
            tokenizedQuery.emplace_back((*i).str());

        if (tokenizedQuery.size() == 1 && tokenizedQuery.front() == "QUIT")
        {
            kill(getpid(), SIGINT);
        }

        if (tokenizedQuery.empty())
        {

            continue;
        }

        if (tokenizedQuery.size() == 1)
        {
            cout << "SYNTAX ERROR" << endl;
            continue;
        }

        doCommand();
    }
}