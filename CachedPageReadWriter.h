#pragma once

#include <vector>
#include <map>
#include <list>
#include <set>

#include "PageReadWriter.h"
#include "GlobalConfiguration.h"
#include "DatabaseNode.h"

class CachedPageReadWriter : public PageReadWriter
{
public:
    enum OpType {
	INSERT,
	DELETE,
	NONE
    };

    CachedPageReadWriter(PageReadWriter *source, GlobalConfiguration *globConf);
    ~CachedPageReadWriter();

    virtual size_t allocatePageNumber();
    virtual void deallocatePageNumber(const size_t &number);

    virtual void read(Page &page);
    virtual void write(const Page &page);

    virtual void close();
    virtual void flush();

    void startOperation(OpType type, const DatabaseNode::Record &key, const DatabaseNode::Record &value);
    void endOperation();

    OpType pendingOperation() const;
    const DatabaseNode::Record &pendingKey() const;
    const DatabaseNode::Record &pendingValue() const;

private:
    static const size_t LOG_ACTION_SIZE = 8;
    static const char LOG_ACTION_CHANGE[LOG_ACTION_SIZE];
    static const char LOG_ACTION_DB_OPEN[LOG_ACTION_SIZE];
    static const char LOG_ACTION_DB_CLOSE[LOG_ACTION_SIZE];
    static const char LOG_ACTION_CHECKPOINT[LOG_ACTION_SIZE];
    static const char LOG_ACTION_DELETE[LOG_ACTION_SIZE];
    static const char LOG_ACTION_INSERT[LOG_ACTION_SIZE];
    static const char LOG_ACTION_COMMIT[LOG_ACTION_SIZE];

    static const size_t LOG_SEEK_DELIM_SIZE = 1;
    static const char LOG_SEEK_DELIM[LOG_SEEK_DELIM_SIZE];
    static const size_t CHECKPOINT_THRESHOLD = 1000;

    GlobalConfiguration *m_globConf;
    PageReadWriter *m_source;
    std::vector<Page *> m_cache;
    std::vector<bool> m_isDirty;
    std::set<size_t> m_pinnedCells;
    std::map<size_t, size_t> m_posInCache;
    std::list<size_t> m_lruList;
    int m_logFd;
    size_t m_writesCounter;
    bool m_inOperation;

    OpType m_pendingOperation;
    DatabaseNode::Record m_pendingKey, m_pendingValue;

    size_t freeCachePosition();
    void flushCacheCell(size_t cachePos);
    void writeLogStumb(size_t toSkip);
};
