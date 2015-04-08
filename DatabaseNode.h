#pragma once

#include <vector>

#include "Page.h"
#include "PageReadWriter.h"
#include "GlobalConfiguration.h"

class DatabaseNode
{
public:
    struct Record
    {
	Record(size_t size = 0, char *data = nullptr);

	bool operator<(const Record &a) const;
	bool operator>(const Record &a) const;
	bool operator==(const Record &a) const;

	static Record rawCopyFrom(const Record &a);

	size_t size;
	char *data;
    };

    DatabaseNode(
	GlobalConfiguration *globConf,
	PageReadWriter &rw,
	size_t rootPageNumber,
	bool needRead);

    ~DatabaseNode();

    void writeToPages(GlobalConfiguration *globConf, PageReadWriter &rw);

    bool isLeaf() const;
    void setIsLeaf(bool val);
    size_t keyCount() const;
    void setKeyCount(size_t newsize);

    std::vector<Record> &keys();
    std::vector<Record> &data();
    std::vector<size_t> &linkedNodesRootPageNumbers();

    size_t rootPage() const;

    void freePages(PageReadWriter &rw);

private:
    bool m_isLeaf;
    size_t m_keyCount;
    std::vector<size_t> m_pageNumbers;
    std::vector<Record> m_keys;
    std::vector<Record> m_data;
    std::vector<size_t> m_linkedNodesRootPageNumbers;

    void readWithExtension(
	GlobalConfiguration *globConf,
	PageReadWriter &rw,
	size_t &curPageNumberId,
	Page *&cur,
	void *res,
	size_t size
    );

    void writeWithExtension(
	GlobalConfiguration *globConf,
	PageReadWriter &rw,
	size_t &curPageNumberId,
	Page *&cur,
	void *res,
	size_t size
    );

    DatabaseNode();
    DatabaseNode(const DatabaseNode &) { }
    void operator=(const DatabaseNode &p) { }
};
