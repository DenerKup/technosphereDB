#pragma once

#include "GlobalConfiguration.h"
#include "PageReadWriter.h"
#include "Bitset.h"

class DiskPageReadWriter : public PageReadWriter
{
public:
    DiskPageReadWriter(const char *file, GlobalConfiguration *globConf);

    // implemented virtual functions
    virtual size_t allocatePageNumber();
    virtual void deallocatePageNumber(const size_t &number);
    void read(Page &p);
    void write(const Page &page);
    void close();
    void flush();

private:
    int m_fd;
    GlobalConfiguration *m_globConf;
    Bitset m_bitset;

    void writeGlobConfAndBitset();
};
