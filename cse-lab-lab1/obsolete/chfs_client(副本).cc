// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define DIR_BREAK "/br/"
#define DIR_SEP "/"


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

std::string join(const stringlist &oList, const std::string &oDelim) {
    std::string aString;
    if (!oList.empty()) {
        for (auto it = oList.begin(); it != std::prev(oList.end()); ++it) {
            aString += (*it);
            aString += oDelim;
        }
        aString += oList.back();
    }
    return aString;
}

template <class T>
T str2num(const std::string &str, bool *success = nullptr) {
    std::stringstream ss;
    T res = 0;

    ss << str;
    ss >> res;

    if (success) {
        *success = ss.good();
    }

    return res;
}

template <class T>
std::string num2str(const T &num) {
    std::stringstream ss;
    std::string str;

    ss << num;
    ss >> str;

    return str;
}

chfs_client::chfs_client() {
    ec = new extent_client();
}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst) {
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum chfs_client::n2i(std::string n) {
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string chfs_client::filename(inum inum) {
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool chfs_client::isfile(inum inum) {
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 *
 * */

bool chfs_client::isdir(inum inum) {
    // Oops! is this still correct when you implement symlink?

    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        return true;
    }
    return false;
}

int chfs_client::getfile(inum inum, fileinfo &fin) {
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int chfs_client::getdir(inum inum, dirinfo &din) {
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx)                                                                                \
    do {                                                                                           \
        if ((xx) != extent_protocol::OK) {                                                         \
            printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__);                                 \
            r = IOERR;                                                                             \
            goto release;                                                                          \
        }                                                                                          \
    } while (0)

// Only support set size of attr
int chfs_client::setattr(inum ino, size_t size) {
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    extent_protocol::attr a;
    if (ec->getattr(ino, a) != extent_protocol::OK) {
        return IOERR;
    }

    std::string content;
    ec->get(ino, content);

    if (content.size() > size) {
        content = content.substr(0, size);
    }
    while (content.size() < size) {
        content.append(" ");
    }

    ec->put(ino, content);
    return r;
}

int chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out) {
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    // Check Parent
    if (!isdir(parent)) {
        return NOENT;
    }

    inum id;
    std::list<dirent> contents;
    int res1 = readdir(parent, contents);
    if (res1 != OK) {
        return IOERR;
    }

    for (auto it = contents.begin(); it != contents.end(); ++it) {
        if (!strcmp(it->name.c_str(), name)) {
            return EXIST;
        }
    }

    ec->create(extent_protocol::T_FILE, id);
    if (id == 0) {
        return NOENT;
    }

    // Modify Parent
    contents.push_back(dirent{std::string(name), id});
    int res2 = writedir(parent, contents);
    if (res2 != OK) {
        return IOERR;
    }

    std::cout << "create file " << id << std::endl;

    ino_out = id;
    return r;
}

int chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out) {
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    if (!isdir(parent)) {
        return NOENT;
    }

    inum id;
    std::list<dirent> contents;
    int res1 = readdir(parent, contents);
    if (res1 != OK) {
        return IOERR;
    }

    for (auto it = contents.begin(); it != contents.end(); ++it) {
        if (!strcmp(it->name.c_str(), name)) {
            return EXIST;
        }
    }

    ec->create(extent_protocol::T_DIR, id);
    if (id == 0) {
        return NOENT;
    }

    // Modify Parent
    contents.push_back(dirent{std::string(name), id});
    int res2 = writedir(parent, contents);
    if (res2 != OK) {
        return IOERR;
    }

    std::cout << "create dir " << id << std::endl;

    ino_out = id;
    return r;
}

int chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out) {
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    if (!isdir(parent)) {
        return NOENT;
    }

    std::list<dirent> contents;
    int res = readdir(parent, contents);
    if (res != OK) {
        return IOERR;
    }

    dirent target{"", 0};
    for (auto it = contents.begin(); it != contents.end(); ++it) {
        if (!strcmp(it->name.c_str(), name)) {
            target = *it;
        }
    }

    if (target.inum == 0) {
        found = false;
        return r;
    }

    found = true;
    ino_out = target.inum;
    return r;
}

int chfs_client::writedir(chfs_client::inum dir, const std::list<chfs_client::dirent> &list) {
    int r = OK;

    std::string str;

    if (!list.empty()) {
        stringlist filenames;
        stringlist inodes;

        for (auto it = list.begin(); it != list.end(); ++it) {
            filenames.push_back(it->name);
            inodes.push_back(num2str(it->inum));
        }
        str = join(filenames, DIR_SEP) + DIR_BREAK + join(inodes, DIR_SEP);
    }

    ec->put(dir, str);
    return r;
}

int chfs_client::readdir(inum dir, std::list<dirent> &list) {
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    std::string str;
    ec->get(dir, str);
    if (str.empty()) {
        list = {};
        return r;
    }

    stringlist list1 = split(str, DIR_BREAK);
    if (list1.size() != 2) {
        return IOERR;
    }

    stringlist filenames = split(list1.front(), DIR_SEP);
    stringlist inodes = split(list1.back(), DIR_SEP);
    if (filenames.size() != inodes.size()) {
        return IOERR;
    }

    auto it1 = filenames.begin();
    auto it2 = inodes.begin();

    list.clear();
    for (; it1 != filenames.end(); ++it1, ++it2) {
        list.push_back({*it1, str2num<chfs_client::inum>(*it2)});
    }
    return r;
}

int chfs_client::read(inum ino, size_t size, off_t off, std::string &data) {
    /*
     * your code goes here.
     * note: read using ec->get().
     */

    extent_protocol::attr a;
    if (ec->getattr(ino, a) != extent_protocol::OK || a.type == 0) {
        return IOERR;
    }

    std::string str;
    ec->get(ino, str);

    if (off >= static_cast<off_t>(str.size())) {
        data.clear();
        return OK;
    }
    data = str.substr(off, size);

    std::cout << "size " << size << "str " << str.size() << " offset " << off << " space "
              << (str.empty() ? -1 : (str.front() == ' ')) << std::endl;
    return OK;
}

int chfs_client::write(inum ino, size_t size, off_t off, const char *data, size_t &bytes_written) {
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    std::cout << "try write " << ino << " sz " << size << " off " << off << std::endl;

    if (!isfile(ino)) {
        return IOERR;
    }

    std::string str;
    ec->get(ino, str);

    std::string str_data(data, size);
    if ((uint32_t) off <= str.size())
        bytes_written = size;
    else {
        bytes_written = off + size - str.size();
        str.resize(off + size, '\0');
    }
    str.replace(off, size, str_data);

    ec->put(ino, str);
    std::cout << "bytes written " << bytes_written << std::endl;

    return r;
}

int chfs_client::unlink(inum parent, const char *name) {
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    if (!isdir(parent)) {
        return NOENT;
    }

    // Modify Parent
    std::list<dirent> contents;
    int res1 = readdir(parent, contents);
    if (res1 != OK) {
        return IOERR;
    }

    dirent target{"", 0};
    for (auto it = contents.begin(); it != contents.end(); ++it) {
        if (!strcmp(it->name.c_str(), name)) {
            target = *it;
            contents.erase(it);
            break;
        }
    }

    if (target.inum == 0) {
        return IOERR;
    }

    ec->remove(target.inum);

    int res2 = writedir(parent, contents);
    if (res2 != OK) {
        return IOERR;
    }

    return r;
}

int chfs_client::readlink(chfs_client::inum ino, std::string &buf) {
    int r = OK;

    if (ec->get(ino, buf) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }
    return r;
}

int chfs_client::symlink(chfs_client::inum parent, const char *name, const char *link,
                         chfs_client::inum &ino_out) {

    if (!isdir(parent)) {
        return NOENT;
    }

    inum id;
    std::list<dirent> contents;
    int res1 = readdir(parent, contents);
    if (res1 != OK) {
        return IOERR;
    }

    for (auto it = contents.begin(); it != contents.end(); ++it) {
        if (!strcmp(it->name.c_str(), name)) {
            return EXIST;
        }
    }

    ec->create(extent_protocol::T_LINK, id);
    if (id == 0) {
        return NOENT;
    }

    if (ec->put(id, std::string(link)) != extent_protocol::OK) {
        return IOERR;
    }

    contents.push_back(dirent{std::string(name), id});
    int res2 = writedir(parent, contents);
    if (res2 != OK) {
        return IOERR;
    }

    std::cout << "create link " << id << std::endl;

    ino_out = id;
    return OK;
}
