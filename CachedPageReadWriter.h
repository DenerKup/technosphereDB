#pragma once

#include <vector>
#include <map>
#include <list>

#include "PageReadWriter.h"
#include "GlobalConfiguration.h"

class CachedPageReadWriter : public PageReadWriter
{
public:
    CachedPageReadWriter(PageReadWriter *source, GlobalConfiguration *globConf);

    virtual size_t allocatePageNumber();
    virtual void deallocatePageNumber(const size_t &number);

    virtual void read(Page &page);
    virtual void write(const Page &page);

    virtual void close();
    virtual void flush();

private:
    static const size_t LOG_ACTION_SIZE = 8;
    static const char LOG_ACTION_CHANGE[LOG_ACTION_SIZE];
    static const char LOG_ACTION_DB_OPEN[LOG_ACTION_SIZE];
    static const char LOG_ACTION_DB_CLOSE[LOG_ACTION_SIZE];
    static const size_t LOG_SEEK_DELIM_SIZE = 1;
    static const char LOG_SEEK_DELIM[LOG_SEEK_DELIM_SIZE];

    GlobalConfiguration *m_globConf;
    PageReadWriter *m_source;
    std::vector<Page *> m_cache;
    std::vector<bool> m_isDirty;
    std::map<size_t, size_t> m_posInCache;
    std::list<size_t> m_lruList;
    int m_logFd;

    size_t freeCachePosition();
    void flushCacheCell(size_t cachePos);
};
