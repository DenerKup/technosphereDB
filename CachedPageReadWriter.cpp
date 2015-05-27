#include "CachedPageReadWriter.h"

#include <string>
#include <cstring>
#include <algorithm>
#include <string>

#include <fcntl.h>
#include <unistd.h>

const char CachedPageReadWriter::LOG_ACTION_CHANGE[CachedPageReadWriter::LOG_ACTION_SIZE] = "CHANGE_";
const char CachedPageReadWriter::LOG_ACTION_DB_OPEN[CachedPageReadWriter::LOG_ACTION_SIZE] = "DB_OPEN";
const char CachedPageReadWriter::LOG_ACTION_DB_CLOSE[CachedPageReadWriter::LOG_ACTION_SIZE] = "DBCLOSE";
const char CachedPageReadWriter::LOG_ACTION_CHECKPOINT[CachedPageReadWriter::LOG_ACTION_SIZE] = "CHCKPNT";
const char CachedPageReadWriter::LOG_ACTION_INSERT[CachedPageReadWriter::LOG_ACTION_SIZE] = "INSERT_";
const char CachedPageReadWriter::LOG_ACTION_DELETE[CachedPageReadWriter::LOG_ACTION_SIZE] = "DELETE_";
const char CachedPageReadWriter::LOG_ACTION_COMMIT[CachedPageReadWriter::LOG_ACTION_SIZE] = "COMMIT_";
const char CachedPageReadWriter::LOG_SEEK_DELIM[CachedPageReadWriter::LOG_SEEK_DELIM_SIZE] = {'|'};

CachedPageReadWriter::CachedPageReadWriter(PageReadWriter *source, GlobalConfiguration *globConf)
    : m_globConf(globConf)
    , m_source(source)
    , m_writesCounter(0)
    , m_inOperation(false)
    , m_pendingOperation(NONE)
    , m_pendingKey(0, nullptr)
    , m_pendingValue(0, nullptr)
{
    if (m_globConf->cacheSize() % m_globConf->pageSize()) {
	throw std::string("Page size should divide cache size.");
    }

    size_t cacheCells = m_globConf->cacheSize() / m_globConf->pageSize();
    m_cache.assign(cacheCells, nullptr);
    m_isDirty.assign(cacheCells, false);
    for (size_t i = 0; i < cacheCells; i++) {
	m_lruList.push_back(i);
    }
    // m_posInCache already empty
    // m_pinnedCells already empty

    bool noJournalBefore = access(globConf->journalPath(), F_OK) == -1;
    m_logFd = open(globConf->journalPath(), O_RDWR | O_CREAT, 0666);
    if (noJournalBefore) {
	::write(m_logFd, LOG_ACTION_CHECKPOINT, LOG_ACTION_SIZE);
	writeLogStumb(LOG_ACTION_SIZE);
    } else {
	size_t recordSize = LOG_ACTION_SIZE + sizeof(size_t) + m_globConf->pageSize();
	lseek(m_logFd, 0, SEEK_END);
	char recordType[LOG_ACTION_SIZE];
	bool isLastOperationFinished = true;
	bool hasSeenOperationEnd = false;
	off_t lastOperationOffset;
	do {
	    off_t curOffset = lseek(m_logFd, -static_cast<off_t>(recordSize), SEEK_CUR);
	    ::read(m_logFd, recordType, LOG_ACTION_SIZE);

	    if (!strcmp(recordType, LOG_ACTION_COMMIT)) {
		hasSeenOperationEnd = true;
	    }

	    if (!strcmp(recordType, LOG_ACTION_INSERT) || !strcmp(recordType, LOG_ACTION_DELETE)) {
		if (!hasSeenOperationEnd) {
		    isLastOperationFinished = false;
		    lastOperationOffset = curOffset;

		    if (!strcmp(recordType, LOG_ACTION_INSERT)) {
			m_pendingOperation = INSERT;

			::read(m_logFd, &(m_pendingKey.size), sizeof(m_pendingKey.size));
			m_pendingKey.data = new char[m_pendingKey.size];
			::read(m_logFd, m_pendingKey.data, m_pendingKey.size);

			::read(m_logFd, &(m_pendingValue.size), sizeof(m_pendingValue.size));
			m_pendingValue.data = new char[m_pendingValue.size];
			::read(m_logFd, m_pendingValue.data, m_pendingValue.size);
		    } else {
			m_pendingOperation = DELETE;

			::read(m_logFd, &(m_pendingKey.size), sizeof(m_pendingKey.size));
			m_pendingKey.data = new char[m_pendingKey.size];
			::read(m_logFd, m_pendingKey.data, m_pendingKey.size);
		    }
		}
	    }

	    lseek(m_logFd, curOffset, SEEK_SET);
	} while (strcmp(recordType, LOG_ACTION_CHECKPOINT));
	// Now pointing begin of check point, lets skip it
	lseek(m_logFd, recordSize, SEEK_CUR);
	while (::read(m_logFd, recordType, LOG_ACTION_SIZE) == LOG_ACTION_SIZE) {
	    off_t curOffset = lseek(m_logFd, 0, SEEK_CUR);
	    if (!strcmp(recordType, LOG_ACTION_CHANGE)) {
		size_t pageNumber;
		::read(m_logFd, &pageNumber, sizeof(pageNumber));
		Page p(pageNumber, m_globConf->pageSize());
		::read(m_logFd, p.rawData(), m_globConf->pageSize());

		m_source->write(p);
	    } else if (!isLastOperationFinished && !strcmp(recordType, LOG_ACTION_INSERT)
		    && !strcmp(recordType, LOG_ACTION_DELETE) && curOffset == lastOperationOffset) {
		// we don't need to restore anything below, lets delete it
		ftruncate(m_logFd, curOffset);
		break;
	    } else {
		lseek(m_logFd, recordSize - LOG_ACTION_SIZE, SEEK_CUR); // skip this entry
	    }
	}
    }

    lseek(m_logFd, 0, SEEK_END);
    ::write(m_logFd, LOG_ACTION_DB_OPEN, LOG_ACTION_SIZE);
    writeLogStumb(LOG_ACTION_SIZE);
}

CachedPageReadWriter::~CachedPageReadWriter()
{
    close();
}

void CachedPageReadWriter::startOperation(OpType type, const DatabaseNode::Record &key, const DatabaseNode::Record &value)
{
    if (type == INSERT) {
	// Assuming here that all of this is less than record size
	::write(m_logFd, LOG_ACTION_INSERT, LOG_ACTION_SIZE);
	::write(m_logFd, &(key.size), sizeof(key.size));
	::write(m_logFd, key.data, key.size);
	::write(m_logFd, &(value.size), sizeof(value.size));
	::write(m_logFd, value.data, value.size);
	writeLogStumb(LOG_ACTION_SIZE + sizeof(key.size) + key.size + sizeof(value.size) + value.size);
    } else if (type == DELETE) {
	::write(m_logFd, LOG_ACTION_DELETE, LOG_ACTION_SIZE);
	::write(m_logFd, &(key.size), sizeof(key.size));
	::write(m_logFd, key.data, key.size);
	writeLogStumb(LOG_ACTION_SIZE + sizeof(key.size) + key.size);
    } else {
	throw std::string("Uknown operation type");
    }
    m_inOperation = true;
}

void CachedPageReadWriter::endOperation()
{
    ::write(m_logFd, LOG_ACTION_COMMIT, LOG_ACTION_SIZE);
    writeLogStumb(LOG_ACTION_SIZE);
    m_inOperation = false;
    m_pinnedCells.clear();
}

size_t CachedPageReadWriter::allocatePageNumber()
{
    return m_source->allocatePageNumber();
}

void CachedPageReadWriter::deallocatePageNumber(const size_t &number)
{
    std::map<size_t, size_t>::iterator it = m_posInCache.find(number);
    if (it != m_posInCache.end()) {
	delete m_cache[it->second];
	m_cache[it->second] = nullptr;
	m_isDirty[it->second] = false;

	m_lruList.erase(std::find(m_lruList.begin(), m_lruList.end(), it->second));
	m_lruList.push_back(it->second); // pushing to oldest to use first

	m_posInCache.erase(it);
    }
    m_source->deallocatePageNumber(number);
}

void CachedPageReadWriter::read(Page &page)
{
    std::map<size_t, size_t>::iterator it = m_posInCache.find(page.number());
    if (it == m_posInCache.end()) { // no page in cache
	size_t freeCachePos = freeCachePosition(); // will do poping if needed
	m_cache[freeCachePos] = new Page(page.number(), m_globConf->pageSize());
	m_posInCache[page.number()] = freeCachePos;
	m_isDirty[freeCachePos] = false;

	m_source->read(*m_cache[freeCachePos]);
    }
    size_t cachePos = m_posInCache[page.number()];
    memcpy(page.rawData(), m_cache[cachePos]->rawData(), m_globConf->pageSize());

    m_lruList.erase(std::find(m_lruList.begin(), m_lruList.end(), cachePos));
    m_lruList.push_front(cachePos);
}

void CachedPageReadWriter::write(const Page &page)
{
    if (m_writesCounter == CHECKPOINT_THRESHOLD) {
	m_writesCounter = 0;
	flush();
    } else {
	m_writesCounter++;
    }

    ::write(m_logFd, LOG_ACTION_CHANGE, LOG_ACTION_SIZE);
    size_t pageNumber = page.number();
    ::write(m_logFd, &pageNumber, sizeof(pageNumber));
    ::write(m_logFd, page.rawData(), m_globConf->pageSize());

    std::map<size_t, size_t>::iterator it = m_posInCache.find(page.number());
    if (it == m_posInCache.end()) { // no page in cache
	size_t freeCachePos = freeCachePosition(); // will do poping if needed
	m_cache[freeCachePos] = new Page(page.number(), m_globConf->pageSize());
	m_posInCache[page.number()] = freeCachePos;
    }
    size_t cachePos = m_posInCache[page.number()];
    m_isDirty[cachePos] = true;
    memcpy(m_cache[cachePos]->rawData(), page.rawData(), m_globConf->pageSize());
    if (m_inOperation) {
	m_pinnedCells.insert(cachePos);
    }

    m_lruList.erase(std::find(m_lruList.begin(), m_lruList.end(), cachePos));
    m_lruList.push_front(cachePos);
}

void CachedPageReadWriter::close()
{
    flush();
    m_source->close();

    ::write(m_logFd, LOG_ACTION_DB_CLOSE, LOG_ACTION_SIZE);
    writeLogStumb(LOG_ACTION_SIZE);

    ::close(m_logFd);

    if (m_pendingKey.data) {
	delete[] m_pendingKey.data;
    }
    if (m_pendingValue.data) {
	delete[] m_pendingValue.data;
    }
}

void CachedPageReadWriter::flushCacheCell(size_t cachePos)
{
    if (m_isDirty[cachePos]) {
	m_source->write(*m_cache[cachePos]);
	m_isDirty[cachePos] = false;
    }
}

void CachedPageReadWriter::flush()
{
    for (const std::pair<size_t, size_t> &p : m_posInCache) {
	flushCacheCell(p.second);
    }
    m_source->flush();

    ::write(m_logFd, LOG_ACTION_CHECKPOINT, LOG_ACTION_SIZE);
    writeLogStumb(LOG_ACTION_SIZE);
}

size_t CachedPageReadWriter::freeCachePosition()
{
    auto it = m_lruList.rbegin();
    for (auto it = m_lruList.rbegin(); it != m_lruList.rend(); it++) {
	size_t cachePos = *it;

	if (m_pinnedCells.count(cachePos)) {
	    continue;
	}

	if (m_cache[cachePos] != nullptr) {
	    flushCacheCell(cachePos);
	    m_posInCache.erase(m_cache[cachePos]->number());
	    delete m_cache[cachePos];
	    m_cache[cachePos] = nullptr;
	}
	return cachePos;
    }
    throw std::string("Everything in cache is pinned. Nothing to throw out!");
}

void CachedPageReadWriter::writeLogStumb(size_t toSkip)
{
    size_t recordSize = LOG_ACTION_SIZE + sizeof(size_t) + m_globConf->pageSize();
    lseek(m_logFd, recordSize - toSkip - LOG_SEEK_DELIM_SIZE, SEEK_CUR); // Seek forward other fields
    ::write(m_logFd, LOG_SEEK_DELIM, LOG_SEEK_DELIM_SIZE);
}

CachedPageReadWriter::OpType CachedPageReadWriter::pendingOperation() const
{
    return m_pendingOperation;
}

const DatabaseNode::Record &CachedPageReadWriter::pendingKey() const
{
    return m_pendingKey;
}

const DatabaseNode::Record &CachedPageReadWriter::pendingValue() const
{
    return m_pendingValue;
}
