#include "chfs_client.h"

#include <QCoreApplication>
#include <QDebug>

using namespace std;

stringlist split(const std::string &oStr, const std::string &oDelim);

int main(int argc, char **argv) {
    QCoreApplication a(argc, argv);

    chfs_client cc;
    list<chfs_client::dirent> dirs;
    size_t size;
    string str;

    // make dir
    chfs_client::inum id1;
    cc.mkdir(1, "dir1", 0, id1);

    // make file1
    chfs_client::inum id2;
    cc.create(id1, "file1", 0, id2);

    cc.write(id2, 10, 10, "1234567890", size);

    cout << size << endl;

    cc.read(id2, 10000, 0, str);

    cout << (str.front() == '\0') << endl;

    return a.exec();
}
