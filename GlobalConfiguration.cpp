#include "GlobalConfiguration.h"
#include "Page.h"

#include <cstring>
#include <string>
#include <unistd.h>

const char GlobalConfiguration::MAGIC[GlobalConfiguration::MAGIC_SIZE] = "MYDB";

GlobalConfiguration::GlobalConfiguration(
    const size_t &desiredPageCount,
    const size_t &desiredPageSize,
    const size_t &desiredRootNodeFirstPageNumber)
    : m_isInitialized(false)
    , m_pageCount(desiredPageCount)
    , m_pageSize(desiredPageSize)
    , m_rootNodeFirstPageNumber(desiredRootNodeFirstPageNumber)
    , m_isReadedFromFile(false)
{
}

void GlobalConfiguration::initialize(
    const size_t &pageCount,
    const size_t &pageSize,
    const size_t &rootNodeFirstPageNumber)
{
    if (m_isInitialized) {
	throw std::string("GlobalConfiguration is already initialized");
    }
    m_isInitialized = true;

    m_pageCount = pageCount;
    m_pageSize = pageSize;
    m_rootNodeFirstPageNumber = rootNodeFirstPageNumber;
}

size_t GlobalConfiguration::desiredPageCount() const
{
    if (m_isInitialized) {
	throw std::string("GlobalConfiguration is already initialized");
    }
    return m_pageCount;
}

size_t GlobalConfiguration::desiredPageSize() const
{
    if (m_isInitialized) {
	throw std::string("GlobalConfiguration is already initialized");
    }
    return m_pageSize;
}

size_t GlobalConfiguration::desiredDatabaseSize() const
{
    if (m_isInitialized) {
	throw std::string("GlobalConfiguration is already initialized");
    }
    return m_pageCount * m_pageSize;
}

size_t GlobalConfiguration::desiredRootNodeFirstPageNumber() const
{
    if (m_isInitialized) {
	throw std::string("GlobalConfiguration is already initialized");
    }
    return m_rootNodeFirstPageNumber;
}

size_t GlobalConfiguration::pageCount() const
{
    if (!m_isInitialized) {
	throw std::string("GlobalConfiguration isn't initialized");
    }
    return m_pageCount;
}

size_t GlobalConfiguration::pageSize() const
{
    if (!m_isInitialized) {
	throw std::string("GlobalConfiguration isn't initialized");
    }
    return m_pageSize;
}

size_t GlobalConfiguration::databaseSize() const
{
    if (!m_isInitialized) {
	throw std::string("GlobalConfiguration isn't initialized");
    }
    return m_pageCount * m_pageSize;
}

size_t GlobalConfiguration::rootNodeFirstPageNumber() const
{
    if (!m_isInitialized) {
	throw std::string("GlobalConfiguration isn't initialized");
    }
    return m_rootNodeFirstPageNumber;
}

bool GlobalConfiguration::isReadedFromFile() const
{
    if (!m_isInitialized) {
	throw std::string("GlobalConfiguration isn't initialized");
    }
    return m_isReadedFromFile;
}

void GlobalConfiguration::readFromFile(int fd)
{
    if (m_isInitialized) {
	throw std::string("GlobalConfiguration is already initialized");
    }
    m_isInitialized = true;
    m_isReadedFromFile = true;

    char magic[MAGIC_SIZE];
    if (read(fd, magic, MAGIC_SIZE) != MAGIC_SIZE) {
	throw std::string("Error reading global configuration");
    }
    if (strcmp(magic, MAGIC)) {
	throw std::string("Invalid magic in database file");
    }
    if (read(fd, &m_pageCount, sizeof(m_pageCount)) != sizeof(m_pageCount)) {
	throw std::string("Error reading global configuration");
    }
    if (read(fd, &m_pageSize, sizeof(m_pageSize)) != sizeof(m_pageSize)) {
	throw std::string("Error reading global configuration");
    }
    if (read(fd, &m_rootNodeFirstPageNumber, sizeof(m_rootNodeFirstPageNumber)) != sizeof(m_rootNodeFirstPageNumber)) {
	throw std::string("Error reading global configuration");
    }
}

void GlobalConfiguration::skipDataOnPage(Page &page) const
{
    if (!m_isInitialized) {
	throw std::string("GlobalConfiguration isn't initialized");
    }

    size_t totalSeek = 0;
    totalSeek += MAGIC_SIZE;
    totalSeek += sizeof(m_pageCount);
    totalSeek += sizeof(m_pageSize);
    totalSeek += sizeof(m_rootNodeFirstPageNumber);
    page.seekForward(totalSeek);
}

void GlobalConfiguration::writeToPage(Page &page) const
{
    if (!m_isInitialized) {
	throw std::string("GlobalConfiguration is already initialized");
    }

    page.seek(0);
    page.write(MAGIC, MAGIC_SIZE);
    page.write(&m_pageCount, sizeof(m_pageCount));
    page.write(&m_pageSize, sizeof(m_pageSize));
    page.write(&m_rootNodeFirstPageNumber, sizeof(m_rootNodeFirstPageNumber));
}
