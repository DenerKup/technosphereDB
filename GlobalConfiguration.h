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
	const size_t &desiredRootNodeFirstPageNumber
    );
    void initialize(
	const size_t &pageCount,
	const size_t &pageSize,
	const size_t &rootNodeFirstPageNumber
    );

    size_t desiredPageCount() const;
    size_t desiredPageSize() const;
    size_t desiredDatabaseSize() const;
    size_t desiredRootNodeFirstPageNumber() const;
    size_t pageCount() const;
    size_t pageSize() const;
    size_t databaseSize() const;
    size_t rootNodeFirstPageNumber() const;

    bool isReadedFromFile() const;

    void readFromFile(int m_fd);
    void skipDataOnPage(Page &page) const;
    void writeToPage(Page &page) const;

private:
    bool m_isInitialized;
    size_t m_pageCount;
    size_t m_pageSize;
    size_t m_rootNodeFirstPageNumber;
    bool m_isReadedFromFile;

    GlobalConfiguration(GlobalConfiguration &) { };
    void operator=(const GlobalConfiguration &) { };
};
