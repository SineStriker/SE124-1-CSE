#include <algorithm>
#include <arpa/inet.h>
#include <cmath>
#include <fstream>
#include <mutex>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "mr_protocol.h"
#include "rpc.h"

using namespace std;

string basedir;

struct Task {
    int taskType;     // should be either Mapper or Reducer
    bool isAssigned;  // has been assigned to a worker
    bool isCompleted; // has been finised by a worker
    int index;        // index to the file
};

struct KeyVal {
    string key;
    string val;
};

class Coordinator {
public:
    Coordinator(const vector<string> &files, int nReduce);
    mr_protocol::status askTask(int, mr_protocol::AskTaskResponse &reply);
    mr_protocol::status submitTask(int taskType, int index, string dir, bool &success);
    bool isFinishedMap();
    bool isFinishedReduce();
    bool Done();

private:
    vector<string> files;
    vector<Task> mapTasks;
    vector<Task> reduceTasks;

    mutex mtx;

    long completedMapCount;
    long completedReduceCount;
    bool isFinished;

    string getFile(int index);
};

// Your code here -- RPC handlers for the worker to call.

mr_protocol::status Coordinator::askTask(int, mr_protocol::AskTaskResponse &reply) {
    // Lab2 : Your code goes here.

    if (!isFinishedMap()) {
        mtx.lock();

        size_t level = ceil(double(mapTasks.size()) / reduceTasks.size());
        size_t i = 0;
        while (i * level < reduceTasks.size()) {
            if (!mapTasks.at(i * level).isAssigned) {
                break;
            }
            i++;
        }

        if (i < reduceTasks.size()) {
           // cout << "ask map task" << endl;
            reply.index = i;
            reply.type = MAP;

            size_t start = i * level;
            size_t end = min((i + 1) * level, mapTasks.size());
            for (size_t j = start; j < end; ++j) {
                mapTasks[j].isAssigned = true;
                reply.filenames.push_back(files.at(j));
            }
        } else {
            reply.index = 0;
            reply.type = NONE;
        }

        mtx.unlock();
    } else if (!isFinishedReduce()) {
        mtx.lock();

        size_t i = 0;
        while (i < reduceTasks.size()) {
            if (!reduceTasks.at(i).isAssigned) {
                break;
            }
            i++;
        }

        if (i < reduceTasks.size()) {
           // cout << "ask reduce task" << endl;
            reply.index = i;
            reply.type = REDUCE;

            reduceTasks[i].isAssigned = true;
        } else {
            reply.index = 0;
            reply.type = NONE;
        }

        mtx.unlock();
    } else {
        reply.index = 0;
        reply.type = NONE;
    }

    return mr_protocol::OK;
}

mr_protocol::status Coordinator::submitTask(int taskType, int index, string dir, bool &success) {
    // Lab2 : Your code goes here.

    if (taskType == MAP) {
        mtx.lock();

       // cout << "submit map task" << endl;

        size_t level = ceil(double(mapTasks.size()) / reduceTasks.size());
        size_t i = index;

        size_t start = i * level;
        size_t end = min((i + 1) * level, mapTasks.size());
        for (size_t j = start; j < end; ++j) {
            mapTasks[j].isCompleted = true;
            completedMapCount++;
        }

        mtx.unlock();
    } else if (taskType == REDUCE) {
        mtx.lock();

       // cout << "submit reduce task" << endl;

        size_t i = index;
        reduceTasks[i].isCompleted = true;
        completedReduceCount++;

        mtx.unlock();
    }

    if (isFinishedReduce()) {
        mtx.lock();

       // cout << "last step" << endl;

        fstream fs;
        vector<KeyVal> intermediate;

        basedir = dir;

        for (size_t k = 0; k < reduceTasks.size(); ++k) {
            fs.open(basedir + "/reduce" + num2str(k) + ".txt", ios::in);
            if (!fs.fail()) {
                string buf;
                readAll(fs, buf);

                stringlist list = split(buf, "\n");

               // cout << "buf " << k << " " << buf.size() << endl;

                bool isKey = true;
                KeyVal kv;
                for (auto line : list) {
                    if (line.empty()) {
                        continue;
                    }
                    if (isKey) {
                        kv.key = line;
                    } else {
                        kv.val = line;
                        intermediate.push_back(kv);
                    }
                    isKey = !isKey;
                }
                fs.close();
            }
        }

        auto cp = [](KeyVal const &a, KeyVal const &b) { return compare(a.key, b.key); };
        sort(intermediate.begin(), intermediate.end(), cp);

        string path = basedir + "/mr-out.txt";
       // cout << path << endl;

        fs.open(path, ios::out);
        // fs.open("./mr-out.txt", ios::out);
        if (fs.fail()) {
            cout << "Fail to write untimate file." << endl;
        } else {
            string buf;
            for (size_t i = 0; i < intermediate.size();) {
                size_t j = i + 1;
                for (; j < intermediate.size() && intermediate[j].key == intermediate[i].key;) {
                    j++;
                }

                int total = 0;
                for (unsigned int k = i; k < j; k++) {
                    total += str2num(intermediate.at(k).val);
                }
                buf += intermediate.at(i).key + " " + num2str(total) + '\n';

                i = j;
            }
            fs.write(buf.c_str(), buf.size());
            fs.close();
        }

        isFinished = true;
        mtx.unlock();
    }

    // cout << "submit finished" << endl;

    return mr_protocol::OK;
}

string Coordinator::getFile(int index) {
    this->mtx.lock();
    string file = this->files[index];
    this->mtx.unlock();
    return file;
}

bool Coordinator::isFinishedMap() {
    bool isFinished = false;
    this->mtx.lock();
    if (this->completedMapCount >= long(this->mapTasks.size())) {
        isFinished = true;
    }
    this->mtx.unlock();
    return isFinished;
}

bool Coordinator::isFinishedReduce() {
    bool isFinished = false;
    this->mtx.lock();
    if (this->completedReduceCount >= long(this->reduceTasks.size())) {
        isFinished = true;
    }
    this->mtx.unlock();
    return isFinished;
}

//
// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
//
bool Coordinator::Done() {
    bool r = false;
    this->mtx.lock();
    r = this->isFinished;
    this->mtx.unlock();
    return r;
}

//
// create a Coordinator.
// nReduce is the number of reduce tasks to use.
//
Coordinator::Coordinator(const vector<string> &files, int nReduce) {
    this->files = files;
    this->isFinished = false;
    this->completedMapCount = 0;
    this->completedReduceCount = 0;

    int filesize = files.size();
    for (int i = 0; i < filesize; i++) {
        this->mapTasks.push_back(Task{mr_tasktype::MAP, false, false, i});
    }
    for (int i = 0; i < nReduce; i++) {
        this->reduceTasks.push_back(Task{mr_tasktype::REDUCE, false, false, i});
    }
}

int main(int argc, char *argv[]) {
    int count = 0;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <port-listen> <inputfiles>...\n", argv[0]);
        exit(1);
    }
    char *port_listen = argv[1];

    setvbuf(stdout, NULL, _IONBF, 0);

    char *count_env = getenv("RPC_COUNT");
    if (count_env != NULL) {
        count = atoi(count_env);
    }

    vector<string> files;
    char **p = &argv[2];
    while (*p) {
        files.push_back(string(*p));
        ++p;
    }

    rpcs server(atoi(port_listen), count);

    Coordinator c(files, REDUCER_COUNT);

    //
    // Lab2: Your code here.
    // Hints: Register "askTask" and "submitTask" as RPC handlers here
    //
    server.reg(mr_protocol::asktask, &c, &Coordinator::askTask);
    server.reg(mr_protocol::submittask, &c, &Coordinator::submitTask);

   // cout << "start" << endl;

    while (!c.Done()) {
        sleep(1);
    }

    // fstream fs;
    // fs.open(basedir + "/result.txt", ios::in);
    // if (!fs.fail()) {
    //     string line;
    //     bool isKey = true;
    //     KeyVal kv;
    //     while (getline(fs, line)) {
    //         if (line.empty()) {
    //             continue;
    //         }
    //         if (isKey) {
    //             kv.key = line;
    //         } else {
    //             kv.val = line;
    //             cout << kv.key << " " << kv.val << endl;
    //         }
    //         isKey = !isKey;
    //     }
    //     fs.close();
    // }

    return 0;
}
