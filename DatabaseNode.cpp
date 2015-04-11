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
	m_rootPageNumber = rootPageNumber;

	Page *p = new Page(rootPageNumber, globConf->pageSize());
	rw.read(*p);

	p->seek(0);
	p->read(&m_isLeaf, sizeof(m_isLeaf));
	p->read(&m_keyCount, sizeof(m_keyCount));

	for (size_t i = 0; i < m_keyCount; i++) {
	    size_t keySize;
	    p->read(&keySize, sizeof(keySize));

	    char *keyValue = new char[keySize];
	    p->read(keyValue, keySize);

	    m_keys.push_back(Record(keySize, keyValue));
	}

	for (size_t i = 0; i < m_keyCount; i++) {
	    size_t dataSize;
	    p->read(&dataSize, sizeof(dataSize));

	    char *dataValue = new char[dataSize];
	    p->read(dataValue, dataSize);

	    m_data.push_back(Record(dataSize, dataValue));
	}

	if (!m_isLeaf) {
	    for (size_t i = 0; i <= m_keyCount; i++) {
		size_t linkedNodePageNumber;
		p->read(&linkedNodePageNumber, sizeof(linkedNodePageNumber));
		m_linkedNodesRootPageNumbers.push_back(linkedNodePageNumber);
	    }
	}

	delete p;
    } else {
	m_rootPageNumber = rootPageNumber;
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

size_t DatabaseNode::spaceOnDisk() const
{
    size_t curSpace = sizeof(m_isLeaf) + sizeof(m_keyCount);
    for (size_t i = 0; i < m_keyCount; i++) {
	curSpace += m_keys[i].size + sizeof(m_keys[i].size);
	curSpace += m_data[i].size + sizeof(m_data[i].size);
    }
    if (!m_isLeaf) {
	curSpace += (m_keyCount + 1) * sizeof(decltype(m_linkedNodesRootPageNumbers.back()));
    }
    return curSpace;
}

size_t DatabaseNode::additionalSpaceFor(const DatabaseNode::Record &key, const DatabaseNode::Record &data) const
{
    size_t result = sizeof(key.size) + key.size;
    result += sizeof(data.size) + data.size;
    if (!m_isLeaf) {
	result += sizeof(decltype(m_linkedNodesRootPageNumbers.back()));
    }
    return result;
}

size_t DatabaseNode::findFirstExceeding(size_t limitSize) const
{
    size_t curSpace = sizeof(m_isLeaf) + sizeof(m_keyCount);
    size_t i = 0;
    curSpace += sizeof(m_keys[0].size) + m_keys[0].size;
    curSpace += sizeof(m_data[0].size) + m_data[0].size;
    if (!m_isLeaf) {
	curSpace += 2 * sizeof(decltype(m_linkedNodesRootPageNumbers.back()));
    }

    while (curSpace <= limitSize && i < m_keyCount) {
	i++;
	curSpace += sizeof(m_keys[i].size) + m_keys[i].size;
	curSpace += sizeof(m_data[i].size) + m_data[i].size;
	if (!m_isLeaf) {
	    curSpace += sizeof(decltype(m_linkedNodesRootPageNumbers.back()));
	}
    }

    return i;
}

void DatabaseNode::writeToPages(GlobalConfiguration *globConf, PageReadWriter &rw)
{
    Page *p = new Page(m_rootPageNumber, globConf->pageSize());

    p->write(&m_isLeaf, sizeof(m_isLeaf));
    p->write(&m_keyCount, sizeof(m_keyCount));

    for (size_t i = 0; i < m_keyCount; i++) {
	p->write(&m_keys[i].size, sizeof(m_keys[i].size));
	p->write(m_keys[i].data, m_keys[i].size);
    }

    for (size_t i = 0; i < m_keyCount; i++) {
	p->write(&m_data[i].size, sizeof(m_data[i].size));
	p->write(m_data[i].data, m_data[i].size);
    }

    if (!m_isLeaf) {
	for (size_t i = 0; i <= m_keyCount; i++) {
	    size_t pageNum = m_linkedNodesRootPageNumbers[i];
	    p->write(&pageNum, sizeof(pageNum));
	}
    }
    rw.write(*p);
    delete p;
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
    return m_rootPageNumber;
}

void DatabaseNode::freePages(PageReadWriter &rw)
{
    rw.deallocatePageNumber(m_rootPageNumber);
}
