#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
    NONE = 0, // this flag means no task needs to be performed at this point
    MAP,
    REDUCE
};

class mr_protocol {
public:
    typedef int status;
    enum xxstatus { OK, RPCERR, NOENT, IOERR };
    enum rpc_numbers {
        asktask = 0xa001,
        submittask,
    };

    struct AskTaskResponse {
        // Lab2: Your definition here.
        int index;
        int type;
        vector<string> filenames;
    };

    struct AskTaskRequest {
        // Lab2: Your definition here.
    };

    struct SubmitTaskResponse {
        // Lab2: Your definition here.
    };

    struct SubmitTaskRequest {
        // Lab2: Your definition here.
    };
};

inline unmarshall &operator>>(unmarshall &u, mr_protocol::AskTaskResponse &a) {
    int size;
    u >> a.index;
    u >> a.type;
    u >> size;
    a.filenames.clear();
    for (int i = 0; i < size; ++i) {
        string str;
        u >> str;
        a.filenames.push_back(str);
    }
    return u;
}

inline marshall &operator<<(marshall &m, mr_protocol::AskTaskResponse a) {
    int size = a.filenames.size();
    m << a.index;
    m << a.type;
    m << size;
    for (int i = 0; i < size; ++i) {
        m << a.filenames.at(i);
    }
    return m;
}

string num2str(int n) {
    stringstream ss;
    string s;
    ss << n;
    ss >> s;
    return s;
}

int str2num(string s) {
    stringstream ss;
    int n;
    ss << s;
    ss >> n;
    return n;
}

typedef list<string> stringlist;
stringlist split(const std::string &oStr, const std::string &oDelim) {
    size_t aCurPos = 0, aNextPos;
    stringlist aVectorString;
    while (aCurPos <= oStr.size()) {
        aNextPos = oStr.find(oDelim, aCurPos);
        if (aNextPos == std::string::npos) {
            aNextPos = oStr.size();
        }
        aVectorString.push_back(oStr.substr(aCurPos, aNextPos - aCurPos));
        aCurPos = aNextPos + oDelim.size();
    }
    return aVectorString;
}

char toLower(char c) {
    if (c >= 'A' && c <= 'Z') {
        return c + ('a' - 'A');
    }
    return c;
}

bool compare(const string &s1, const string &s2) {
    size_t m = min(s1.size(), s2.size());
    for (size_t i = 0; i < m; ++i) {
        char c1 = toLower(s1.at(i));
        char c2 = toLower(s2.at(i));
        if (c1 != c2) {
            return c1 < c2;
        }
    }
    if (s1.size() != s2.size()) {
        return s1.size() < s2.size();
    }
    return s1 < s2;
}

void readAll(fstream &is, string &str) {
    is.seekg(0, is.end);
    int length = is.tellg();
    is.seekg(0, is.beg);

    // allocate memory:
    char *buffer = new char[length];

    // read data as a block:
    is.read(buffer, length);

    // print content:
    str = string(buffer, length);
    delete[] buffer;
}

#endif
