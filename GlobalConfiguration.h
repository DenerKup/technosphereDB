#pragma once

#include <cstddef>

#include "Page.h"

class GlobalConfiguration
{
public:
    static const int MAGIC_SIZE = 5;
    static const char MAGIC[MAGIC_SIZE];

    GlobalConfiguration(
	const size_t &desiredPageCount,
	const size_t &desiredPageSize,
	const size_t &desiredRootNodePageNumber,
	const size_t &desiredCacheSize,
	const char *desiredJournalPath
    );

    ~GlobalConfiguration();

    void initialize(
	const size_t &pageCount,
	const size_t &pageSize,
	const size_t &rootNodePageNumber,
	const size_t &cacheSize,
	const char *journalPath
    );

    size_t desiredPageCount() const;
    size_t desiredPageSize() const;
    size_t desiredDatabaseSize() const;
    size_t desiredRootNodePageNumber() const;
    size_t desiredCacheSize() const;
    char *desiredJournalPath() const;
    size_t pageCount() const;
    size_t pageSize() const;
    size_t databaseSize() const;
    size_t rootNodePageNumber() const;
    size_t cacheSize() const;
    char *journalPath() const;

    void setRootNodePageNumber(const size_t &newRootNodePageNumber);

    bool isReadedFromFile() const;

    void readFromFile(int m_fd);
    void skipDataOnPage(Page &page) const;
    void writeToPage(Page &page) const;

private:
    bool m_isInitialized;
    size_t m_pageCount;
    size_t m_pageSize;
    size_t m_rootNodePageNumber;
    size_t m_cacheSize;
    char *m_journalPath;
    bool m_isReadedFromFile;

    GlobalConfiguration(GlobalConfiguration &) { };
    void operator=(const GlobalConfiguration &) { };
};
