#include "DatabaseNode.h"

#include <string>
#include <climits>
#include <cstring>

DatabaseNode::Record::Record(size_t _size, char *_data)
    : size(_size)
    , data(_data)
{
}

bool DatabaseNode::Record::operator<(const DatabaseNode::Record &a) const
{
    if (size != a.size) {
	return size < a.size;
    }
    if (size) {
	return memcmp(data, a.data, size) < 0;
    } else {
	return false;
    }
}

bool DatabaseNode::Record::operator>(const DatabaseNode::Record &a) const
{
    if (size != a.size) {
	return size > a.size;
    }
    if (size) {
	return memcmp(data, a.data, size) > 0;
    } else {
	return false;
    }
}

bool DatabaseNode::Record::operator==(const DatabaseNode::Record &a) const
{
    if (size != a.size) {
	return false;
    }
    if (size) {
	return memcmp(data, a.data, size) == 0;
    } else {
	return true;
    }
}

DatabaseNode::Record DatabaseNode::Record::rawCopyFrom(const DatabaseNode::Record &a)
{
    DatabaseNode::Record res(a.size, new char[a.size]);
    memcpy(res.data, a.data, a.size);
    return res;
}

DatabaseNode::DatabaseNode(GlobalConfiguration *globConf, PageReadWriter &rw, size_t rootPageNumber, bool needRead)
{
    if (needRead) {
	Page *cur = new Page(rootPageNumber, globConf->pageSize());
	rw.read(*cur);

	cur->seek(0);
	size_t curPageNumId = 0;
	cur->read(&m_isLeaf, sizeof(m_isLeaf));
	cur->read(&m_keyCount, sizeof(m_keyCount));

	size_t pageNumbersSize;
	cur->read(&pageNumbersSize, sizeof(pageNumbersSize));
	for (size_t i = 0; i < pageNumbersSize; i++) {
	    size_t pageNum;
	    readWithExtension(globConf, rw, curPageNumId, cur, &pageNum, sizeof(pageNum));
	    m_pageNumbers.push_back(pageNum);
	}

	for (size_t i = 0; i < m_keyCount; i++) {
	    size_t keySize;
	    readWithExtension(globConf, rw, curPageNumId, cur, &keySize, sizeof(keySize));
	    char *keyValue = new char[keySize];
	    readWithExtension(globConf, rw, curPageNumId, cur, keyValue, keySize);
	    m_keys.push_back(Record(keySize, keyValue));
	}

	for (size_t i = 0; i < m_keyCount; i++) {
	    size_t dataSize;
	    readWithExtension(globConf, rw, curPageNumId, cur, &dataSize, sizeof(dataSize));
	    char *dataValue = new char[dataSize];
	    readWithExtension(globConf, rw, curPageNumId, cur, dataValue, dataSize);
	    m_data.push_back(Record(dataSize, dataValue));
	}

	if (!m_isLeaf) {
	    for (size_t i = 0; i <= m_keyCount; i++) {
		size_t linkedNodePageNumber;
		readWithExtension(globConf, rw, curPageNumId, cur, &linkedNodePageNumber, sizeof(linkedNodePageNumber));
		m_linkedNodesRootPageNumbers.push_back(linkedNodePageNumber);
	    }
	}

	delete cur;
    } else {
	m_pageNumbers.push_back(rootPageNumber);
	m_isLeaf = true;
	m_keyCount = 0;
    }
}

DatabaseNode::~DatabaseNode()
{
    for (size_t i = 0; i < m_keyCount; i++) {
	delete[] m_keys[i].data;
	delete[] m_data[i].data;
    }
}

void DatabaseNode::writeToPages(GlobalConfiguration *globConf, PageReadWriter &rw)
{
    size_t neededPlace = 0;
    neededPlace += sizeof(m_isLeaf);
    neededPlace += sizeof(m_keyCount);
    neededPlace += sizeof(size_t); // pageNumbersSize
    for (size_t i = 0; i < m_keyCount; i++) {
	neededPlace += sizeof(size_t); // key size of size
	neededPlace += m_keys[i].size; // key data size
	neededPlace += sizeof(size_t); // data size of size
	neededPlace += m_data[i].size; // data data size
    }
    if (!m_isLeaf) {
	neededPlace += sizeof(size_t) * (m_keyCount + 1); // links;
    }

    size_t needPageNumbersSize = neededPlace / globConf->pageSize();
    while (needPageNumbersSize * globConf->pageSize() <
	neededPlace + needPageNumbersSize * sizeof(size_t) + sizeof(size_t)) {
	needPageNumbersSize++;
    }

    while (needPageNumbersSize > m_pageNumbers.size()) {
	m_pageNumbers.push_back(rw.allocatePageNumber());
    }
    while (needPageNumbersSize < m_pageNumbers.size()) {
	rw.deallocatePageNumber(m_pageNumbers.back());
	m_pageNumbers.pop_back();
    }

    size_t curPageNumId = 0;
    Page *cur = new Page(m_pageNumbers[0], globConf->pageSize());

    cur->write(&m_isLeaf, sizeof(m_isLeaf));
    cur->write(&m_keyCount, sizeof(m_keyCount));
    {
	size_t pageNumbersSize = m_pageNumbers.size();
	cur->write(&pageNumbersSize, sizeof(pageNumbersSize));
    }

    for (size_t i = 0; i < m_pageNumbers.size(); i++) {
	size_t pageNum = m_pageNumbers[i];
	writeWithExtension(globConf, rw, curPageNumId, cur, &pageNum, sizeof(pageNum));
    }

    for (size_t i = 0; i < m_keyCount; i++) {
	writeWithExtension(globConf, rw, curPageNumId, cur, &m_keys[i].size, sizeof(m_keys[i].size));
	writeWithExtension(globConf, rw, curPageNumId, cur, m_keys[i].data, m_keys[i].size);
    }

    for (size_t i = 0; i < m_keyCount; i++) {
	writeWithExtension(globConf, rw, curPageNumId, cur, &m_data[i].size, sizeof(m_data[i].size));
	writeWithExtension(globConf, rw, curPageNumId, cur, m_data[i].data, m_data[i].size);
    }

    if (!m_isLeaf) {
	for (size_t i = 0; i <= m_keyCount; i++) {
	    size_t pageNum = m_linkedNodesRootPageNumbers[i];
	    writeWithExtension(globConf, rw, curPageNumId, cur, &pageNum, sizeof(pageNum));
	}
    }
    rw.write(*cur);
    delete cur;
}

void DatabaseNode::readWithExtension(
    GlobalConfiguration *globConf,
    PageReadWriter &rw,
    size_t &curPageNumId,
    Page *&cur,
    void *_res,
    size_t size
)
{
    char *res = reinterpret_cast<char *>(_res);
    size_t readedSize = 0;
    while (readedSize < size) {
	if (cur->freeSpace() == 0) {
	    curPageNumId++;
	    if (curPageNumId >= m_pageNumbers.size()) {
		throw std::string("Reached end of allocated pages");
	    }
	    Page *next = new Page(m_pageNumbers[curPageNumId], globConf->pageSize());
	    rw.read(*next);
	    delete cur;
	    cur = next;
	}
	size_t toRead = std::min(size - readedSize, cur->freeSpace());
	cur->read(res + readedSize, toRead);
	readedSize += toRead;
    }
}

void DatabaseNode::writeWithExtension(
    GlobalConfiguration *globConf,
    PageReadWriter &rw,
    size_t &curPageNumId,
    Page *&cur,
    void *_res,
    size_t size
)
{
    char *res = reinterpret_cast<char *>(_res);
    size_t writtenSize = 0;
    while (writtenSize < size) {
	if (cur->freeSpace() == 0) {
	    curPageNumId++;
	    if (curPageNumId >= m_pageNumbers.size()) {
		throw std::string("Reached end of allocated pages");
	    }
	    Page *q = new Page(m_pageNumbers[curPageNumId], globConf->pageSize());
	    rw.write(*cur);
	    delete cur;
	    cur = q;
	}
	size_t toWrite = std::min(size - writtenSize, cur->freeSpace());
	cur->write(res + writtenSize, toWrite);
	writtenSize += toWrite;
    }
}

bool DatabaseNode::isLeaf() const
{
    return m_isLeaf;
}

void DatabaseNode::setIsLeaf(bool val)
{
    m_isLeaf = val;
}

size_t DatabaseNode::keyCount() const
{
    return m_keyCount;
}

void DatabaseNode::setKeyCount(size_t newSize)
{
    m_keyCount = newSize;
}

std::vector<DatabaseNode::Record> &DatabaseNode::data()
{
    return m_data;
}

std::vector<DatabaseNode::Record> &DatabaseNode::keys()
{
    return m_keys;
}

std::vector<size_t> &DatabaseNode::linkedNodesRootPageNumbers()
{
    return m_linkedNodesRootPageNumbers;
}

size_t DatabaseNode::rootPage() const
{
    return m_pageNumbers[0];
}

void DatabaseNode::freePages(PageReadWriter &rw)
{
    for (size_t i = 0; i < m_pageNumbers.size(); i++) {
	rw.deallocatePageNumber(m_pageNumbers[i]);
    }
    m_pageNumbers.clear();
}
