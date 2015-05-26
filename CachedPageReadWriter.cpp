#include "CachedPageReadWriter.h"

#include <string>
#include <cstring>
#include <algorithm>

#include <fcntl.h>
#include <unistd.h>

const char CachedPageReadWriter::LOG_ACTION_CHANGE[CachedPageReadWriter::LOG_ACTION_SIZE] = "CHANGE_";
const char CachedPageReadWriter::LOG_ACTION_DB_OPEN[CachedPageReadWriter::LOG_ACTION_SIZE] = "DB_OPEN";
const char CachedPageReadWriter::LOG_ACTION_DB_CLOSE[CachedPageReadWriter::LOG_ACTION_SIZE] = "DBCLOSE";
const char CachedPageReadWriter::LOG_SEEK_DELIM[CachedPageReadWriter::LOG_SEEK_DELIM_SIZE] = {'|'};

CachedPageReadWriter::CachedPageReadWriter(PageReadWriter *source, GlobalConfiguration *globConf)
    : m_globConf(globConf)
    , m_source(source)
{
    if (m_globConf->cacheSize() % m_globConf->pageSize()) {
	throw std::string("Page size should divide cache size.");
    }

    m_logFd = open(globConf->journalPath(), O_RDWR | O_CREAT, 0666);
    //TODO: Do restore of database here...

    lseek(m_logFd, 0, SEEK_END);
    ::write(m_logFd, LOG_ACTION_DB_OPEN, LOG_ACTION_SIZE);
    lseek(m_logFd, sizeof(size_t) + m_globConf->pageSize() - LOG_SEEK_DELIM_SIZE, SEEK_CUR); // Seek forward other fields
    ::write(m_logFd, LOG_SEEK_DELIM, LOG_SEEK_DELIM_SIZE);

    size_t cacheCells = m_globConf->cacheSize() / m_globConf->pageSize();
    m_cache.assign(cacheCells, nullptr);
    m_isDirty.assign(cacheCells, false);
    for (size_t i = 0; i < cacheCells; i++) {
	m_lruList.push_back(i);
    }
    // m_posInCache already empty
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
    char oldData[m_globConf->pageSize()];
    memcpy(oldData, page.rawData(), m_globConf->pageSize());

    std::map<size_t, size_t>::iterator it = m_posInCache.find(page.number());
    if (it == m_posInCache.end()) { // no page in cache
	size_t freeCachePos = freeCachePosition(); // will do poping if needed
	m_cache[freeCachePos] = new Page(page.number(), m_globConf->pageSize());
	m_posInCache[page.number()] = freeCachePos;
    }
    size_t cachePos = m_posInCache[page.number()];
    m_isDirty[cachePos] = true;
    memcpy(m_cache[cachePos]->rawData(), page.rawData(), m_globConf->pageSize());
    flushCacheCell(cachePos);

    m_lruList.erase(std::find(m_lruList.begin(), m_lruList.end(), cachePos));
    m_lruList.push_front(cachePos);

    ::write(m_logFd, LOG_ACTION_CHANGE, LOG_ACTION_SIZE);
    size_t pageNumber = page.number();
    ::write(m_logFd, &pageNumber, sizeof(pageNumber));
    ::write(m_logFd, oldData, m_globConf->pageSize());
}

void CachedPageReadWriter::close()
{
    flush();
    m_source->close();

    ::write(m_logFd, LOG_ACTION_DB_CLOSE, LOG_ACTION_SIZE);
    lseek(m_logFd, sizeof(size_t) + m_globConf->pageSize() - LOG_SEEK_DELIM_SIZE, SEEK_CUR); // Seek forward other fields
    ::write(m_logFd, LOG_SEEK_DELIM, LOG_SEEK_DELIM_SIZE);

    ::close(m_logFd);
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
}

size_t CachedPageReadWriter::freeCachePosition()
{
    size_t cachePos = m_lruList.back();

    if (m_cache[cachePos] != nullptr) {
	flushCacheCell(cachePos);
	m_posInCache.erase(m_cache[cachePos]->number());
	delete m_cache[cachePos];
	m_cache[cachePos] = nullptr;
    }

    return cachePos;
}
