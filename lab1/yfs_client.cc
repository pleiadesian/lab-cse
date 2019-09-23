// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client()
{
    ec = new extent_client();

}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
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

bool
yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
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

int
yfs_client::getdir(inum inum, dirinfo &din)
{
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


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */



    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    printf("\n\n\n%s\n/////////", name);
    /* check parent node */
    if (!isdir(parent)) {
        printf("-------not dir\n");
        return IOERR;
    }

    printf("\n1\n");
    /* check filename */
    if (name == NULL) {
        printf("-------name null\n");
        return IOERR;
    }
    std::string filename = std::string(name);
    if (filename.empty() || !filename.find('/') == std::string::npos || !filename.find('\0') == std::string::npos) {
        printf("-------name error\n");
        return IOERR;
    }

    printf("\n2\n");
    std::list<dirent> filelist;
    r = readdir(parent, filelist);
    if (r != OK) {
        printf("-------readdir error\n");
        return r;
    }

    printf("\n3\n");
    bool found = false;
    inum temp_inum;
    lookup(parent, name, found, temp_inum);
    if (found) {
        printf("-------exist\n");
        return EXIST;
    }

    dirent de;
    de.name = filename;
    if (ec->create(extent_protocol::T_FILE, ino_out)) {
        printf("-------create file error\n");
        return IOERR;
    }
    de.inum = ino_out;

    printf("\n4\n");
    std::string buf, sde;
    filelist.push_back(de);
//    std::ostringstream oss;
////    char file[MAXFILE]
    for (std::list<dirent>::iterator it = filelist.begin() ; it != filelist.end() ; it++) {
        fix_dir_entry fde;
        it->name.copy(fde.name, it->name.length());
        fde.name_size = it->name.length();
        fde.inum = it->inum;
//        sprintf(buf, "%08d%s%020d", it->name.length(), it->name, it->inum);
        sde.assign((char *)&fde, sizeof(fix_dir_entry));
        buf += sde;
//        oss.write(std::string(it->name.length()), sizeof(long));
//        oss.write(it->name, it->name.length());
//        oss.write(it->inum, sizeof(inum));
//        oss << it->name << '\0' << it->inum << '\0';
    }
    printf("\n5\n");
//    std::cout<< "\n\nwrite filename:\n" << buf << "\n\n";
    if (ec->put(parent, buf) != OK) {
        printf("-------put error\n");
        return IOERR;
    }

    printf("\n6 %d\n", r);
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    printf("\nlookup\n%s\n\n", name);
    std::list<dirent> de_list;
    readdir(parent, de_list);

    for (std::list<dirent>::iterator it = de_list.begin() ; it != de_list.end() ; it++) {

        std::string sname;
//        printf("------cmp\n%s\n---------\n%s\n", it->name.c_str(), name);
        if (it->name == sname.assign(name, sizeof(name))) {
            found = true;
            ino_out = it->inum;
            break;
        }

    }

//    std::string buf, snamesize, sname, sinum;
//    extent_protocol::attr attr;
//    if (ec->get(parent, buf) != OK || ec->getattr(parent, attr)) {
//        return IOERR;
//    }
//
//    if (attr.type != extent_protocol::T_DIR) {
//        return IOERR;
//    }
//
//    attr.atime = (unsigned int)time(NULL);
//
//    const char *char_buf = buf.c_str();
//    int filesize = buf.length();
//    int entry_num = filesize / sizeof(fix_dir_entry);
//
//    found = false;
//    for (int i = 0 ; i < entry_num ; i++) {
//        fix_dir_entry fde;
//        memcpy(&fde, char_buf + i * sizeof(fix_dir_entry), sizeof(fix_dir_entry));
//        printf("------cmp\n%s\n---------\n%s\n", fde.name, name);
//        if (!strncmp(fde.name, name, fde.name_size)) {
//            found = true;
//            ino_out = fde.inum;
//            break;
//        }
//    }



//    std::string buf, snamesize, sname, sinum;
//    if (ec->get(parent, buf) != OK) {
//        return IOERR;
//    }
////    std::cout<<"\n\ndir:"<<buf<<"\n\n";
//
//    std::stringstream ss(buf);
//    std::string filename = std::string(name);
//    found = false;
//    while (ss.read(snamesize, sizeof(long))) {
//        int size = atoi(snamesize.c_str());
//        if (!ss.read(sname, size)) {
//            return IOERR;
//        }
//        if (!ss.read(sinum, sizeof(inum))) {
//            return IOERR;
//        }
//        dirent de;
//        de.name = sname;
//        de.inum = (inum)sinum;
//        if (filename == sname) {
//            found = true;
//            ino_out = de.inum;
//            break;
//        }
//    }
    //    while(getline(ss, sname, '\0')) {
//        if (!getline(ss, sinum, '\0')) {
//            break;
//        }
//        dirent de;
//        de.name = sname;
//        de.inum = atoi(sinum.c_str());
//        if (filename == sname) {
//            found = true;
//            ino_out = de.inum;
//            break;
//        }
//    }

    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string buf, snamesize, sname, sinum;
    extent_protocol::attr attr;
    if (ec->get(dir, buf) != OK || ec->getattr(dir, attr)) {
        printf("-------getattr error\n");
        return IOERR;
    }

    if (attr.type != extent_protocol::T_DIR) {
        printf("-------type error\n");
        return IOERR;
    }

    attr.atime = (unsigned int)time(NULL);

    const char *char_buf = buf.c_str();
    int filesize = buf.length();
    int entry_num = filesize / sizeof(fix_dir_entry);

    for (int i = 0 ; i < entry_num ; i++) {
        fix_dir_entry fde;
        memcpy(&fde, char_buf + i * sizeof(fix_dir_entry), sizeof(fix_dir_entry));
        dirent de;
        de.name = std::string(fde.name).substr(0, fde.name_size);
        de.inum = fde.inum;
        list.push_back(de);
    }
//
//    std::istringstream ss(buf);
//    while (ss.read(snamesize, sizeof(long))) {
//        int size = atoi(snamesize.c_str());
//        if (!ss.read(sname, size)) {
//            return IOERR;
//        }
//        if (!ss.read(sinum, sizeof(inum))) {
//            return IOERR;
//        }
//        dirent de;
//        de.name = sname;
//        de.inum = (inum)sinum;
//        list.push_back(de);
//    }
    //    while(getline(ss, sname, '\0')) {
//        if (!getline(ss, sinum, '\0')) {
//            break;
//        }
//        dirent de;
//        de.name = sname;
//        de.inum = atoi(sinum.c_str());
//        list.push_back(de);
//    }

    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */

    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    return r;
}

