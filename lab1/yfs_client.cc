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

    std::string buf;
    if (ec->get(ino, buf) != extent_protocol::OK) {
        return IOERR;
    }

    buf.resize(size);
//    size_t curr_size = buf.size();
//
//    if (size == 0) {
//        ec->remove(ino);
//        return r;
//    }
//
//    if (curr_size > size) {
//        buf = buf.substr(0, size);
//    }
//
//    if (curr_size < size) {
//        int diff = size - curr_size;
//        while (diff > 0) {
//            buf += '\0';
//            diff--;
//        }
//    }

    if (ec->put(ino, buf) != extent_protocol::OK) {
        return IOERR;
    }

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
        return IOERR;
    }
    std::string filename = std::string(name);
    if (filename.empty() || !filename.find('/') == std::string::npos || !filename.find('\0') == std::string::npos) {
        return IOERR;
    }

    std::string buf;
    if (ec->get(parent, buf) != extent_protocol::OK) {
        return IOERR;
    }

    bool found = false;
    inum temp_inum;
    lookup(parent, name, found, temp_inum);
    if (found) {
        return EXIST;
    }

    /* create a new file */
    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK) {
        return IOERR;
    }

    /* generate fix size direct entry */
    std::string sde;
    fix_dir_entry fde;
    fde.name_size = (int) strlen(name);
    memcpy(fde.name, name, fde.name_size);
    fde.inum = ino_out;

    /* convert entry to string */
    sde.assign((char *)&fde, sizeof(fix_dir_entry));
    buf += sde;

    /* write entry to parent dir */
    if (ec->put(parent, buf) != extent_protocol::OK) {
        return IOERR;
    }

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
    bool exist = false;
    inum inum_out;
    lookup(parent, name, exist, inum_out);
    if (exist) {
        return EXIST;
    }

    /* create a new dir */
    if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK) {
        return IOERR;
    }

    /* get parent dir entries */
    std::string buf;
    if (ec->get(parent, buf) != extent_protocol::OK) {
        return IOERR;
    }

    /* generate fix size direct entry */
    std::string sde;
    fix_dir_entry fde;
    fde.name_size = (int) strlen(name);
    memcpy(fde.name, name, fde.name_size);
    fde.inum = ino_out;

    /* convert entry to string */
    sde.assign((char *)&fde, sizeof(fix_dir_entry));
    buf += sde;

    /* write entry to parent dir */
    if (ec->put(parent, buf) != extent_protocol::OK) {
        return IOERR;
    }

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

    printf("\npass readdir\n");

    found = false;
    for (std::list<dirent>::iterator it = de_list.begin() ; it != de_list.end() ; it++) {
        printf("\nlopping\n");
        std::string sname;
//        printf("------cmp\n%s\n---------\n%s\n", it->name.c_str(), name);
        if (it->name == sname.assign(name, strlen(name))) {
            found = true;
            ino_out = it->inum;
            break;
        }

    }

    printf("\nend loop\n");


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
    printf("\na\n");
    std::string buf, snamesize, sname, sinum;
    extent_protocol::attr attr;
    if (ec->get(dir, buf) != extent_protocol::OK || ec->getattr(dir, attr) != extent_protocol::OK) {
        printf("-------getattr error\n");
        return IOERR;
    }

    printf("\nb\n");
    if (attr.type != extent_protocol::T_DIR) {
        printf("-------type error\n");
        return IOERR;
    }

    attr.atime = (unsigned int)time(NULL);

    const char *char_buf = buf.c_str();
    int filesize = (int)buf.size();
    int entry_num = filesize / sizeof(fix_dir_entry);

    printf("\nc sizeof parent:%d sizeof charbuf:%d entrysize:%d  entry num:%d\n", attr.size, (int)strlen(char_buf), (int)sizeof(fix_dir_entry), (int)entry_num);
    printf("\n-------------\n %s \n\n", char_buf);
    for (int i = 0 ; i < entry_num ; i++) {
        printf("\nd\n");
        fix_dir_entry fde;
        memcpy(&fde, char_buf + i * sizeof(fix_dir_entry), sizeof(fix_dir_entry));
        dirent de;
        printf("\ne name:%s size:%d\n", fde.name, fde.name_size);
        de.name.assign(fde.name, fde.name_size);
        de.inum = fde.inum;
        printf("\nf\n");
        list.push_back(de);
    }

    printf("\nd\n");
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
    std::string buf;
    if (ec->get(ino, buf) != extent_protocol::OK) {
        return IOERR;
    }

    data.erase();
    if (off >= (off_t)buf.length()) {
        return r;
    }

    data = buf.substr(off, size);

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

    std::string buf;
    if (ec->get(ino, buf) != extent_protocol::OK) {
        return IOERR;
    }

    std::string sdata;
    sdata.assign(data, size);

    if (off > (off_t)buf.size()) {
        size_t diff = (size_t)off - buf.size();
        buf.resize(off + size, '\0');
        buf.replace(off, size, sdata);
        bytes_written = diff + size;
    } else {
        buf.replace(off, size, sdata);
        bytes_written = size;
    }

    if (ec->put(ino, buf) != extent_protocol::OK) {
        return IOERR;
    }

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
    /* check if file exists */
    bool exist = false;
    inum inum_out;
    lookup(parent, name, exist, inum_out);
    if (!exist) {
        return NOENT;
    }

    if (ec->remove(inum_out) != extent_protocol::OK) {
        return IOERR;
    }

    std::list<dirent> de_list;
    readdir(parent, de_list);
    std::string buf;
    for (std::list<dirent>::iterator it = de_list.begin() ; it != de_list.end() ; it++) {
        /* pass the removed entry */
        if (it->inum == inum_out) {
            continue;
        }

        /* generate fix size direct entry */
        std::string sde;
        fix_dir_entry fde;
        fde.name_size = (int)it->name.size();
        memcpy(fde.name, it->name.c_str(), fde.name_size);
        fde.inum = it->inum;

        /* convert entry to string */
        sde.assign((char *)&fde, sizeof(fix_dir_entry));
        buf += sde;
    }

    /* write entry to parent dir */
    if (ec->put(parent, buf) != extent_protocol::OK) {
        return IOERR;
    }

    return r;
}

int yfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out) {
    int r = OK;

    bool exist = false;
    inum inum_out;
    lookup(parent, name, exist, inum_out);
    if (exist) {
        return EXIST;
    }

    /* create a new dir */
    if (ec->create(extent_protocol::T_SYMLINK, ino_out) != extent_protocol::OK) {
        return IOERR;
    }

    /* write link to symlink file */
    std::string slink = std::string(link);
    ec->put(ino_out, slink);

    /* get parent dir entries */
    std::string buf;
    if (ec->get(parent, buf) != extent_protocol::OK) {
        return IOERR;
    }

    /* generate fix size direct entry */
    std::string sde;
    fix_dir_entry fde;
    fde.name_size = (int)strlen(name);
    memcpy(fde.name, name, fde.name_size);
    fde.inum = ino_out;

    /* convert entry to string */
    sde.assign((char *)&fde, sizeof(fix_dir_entry));
    buf += sde;

    /* write entry to parent dir */
    if (ec->put(parent, buf) != extent_protocol::OK) {
        return IOERR;
    }

    return r;
}

int yfs_client::readlink(inum ino, std::string &data) {
    int r = OK;

    if (ec->get(ino, data) != extent_protocol::OK) {
        return IOERR;
    }

    return r;
}

