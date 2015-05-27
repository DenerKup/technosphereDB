#include "GlobalConfiguration.h"
#include "Page.h"

#include <cstring>
#include <string>
#include <unistd.h>

const char GlobalConfiguration::MAGIC[GlobalConfiguration::MAGIC_SIZE] = "MYDB";

GlobalConfiguration::GlobalConfiguration(
    const size_t &desiredPageCount,
    const size_t &desiredPageSize,
    const size_t &desiredRootNodePageNumber,
    const size_t &desiredCacheSize,
    const char *journalPath)
    : m_isInitialized(false)
    , m_pageCount(desiredPageCount)
    , m_pageSize(desiredPageSize)
    , m_rootNodePageNumber(desiredRootNodePageNumber)
    , m_cacheSize(desiredCacheSize)
    , m_journalPath(strdup(journalPath))
    , m_isReadedFromFile(false)
{
}

GlobalConfiguration::~GlobalConfiguration()
{
    free(m_journalPath);
}

void GlobalConfiguration::initialize(
    const size_t &pageCount,
    const size_t &pageSize,
    const size_t &rootNodePageNumber,
    const size_t &cacheSize,
    const char *journalPath)
{
    if (m_isInitialized) {
	throw std::string("GlobalConfiguration is already initialized");
    }
    m_isInitialized = true;

    m_pageCount = pageCount;
    m_pageSize = pageSize;
    m_rootNodePageNumber = rootNodePageNumber;
    m_cacheSize = cacheSize;
    char *oldMem = m_journalPath;
    m_journalPath = strdup(journalPath);
    if (oldMem) {
	free(oldMem);
    }
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

size_t GlobalConfiguration::desiredRootNodePageNumber() const
{
    if (m_isInitialized) {
	throw std::string("GlobalConfiguration is already initialized");
    }
    return m_rootNodePageNumber;
}

size_t GlobalConfiguration::desiredCacheSize() const
{
    if (m_isInitialized) {
	throw std::string("GlobalConfiguration is already initialized");
    }
    return m_cacheSize;
}

char *GlobalConfiguration::desiredJournalPath() const
{
    if (m_isInitialized) {
	throw std::string("GlobalConfiguration is already initialized");
    }
    return m_journalPath;
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

size_t GlobalConfiguration::rootNodePageNumber() const
{
    if (!m_isInitialized) {
	throw std::string("GlobalConfiguration isn't initialized");
    }
    return m_rootNodePageNumber;
}

size_t GlobalConfiguration::cacheSize() const
{
    if (!m_isInitialized) {
	throw std::string("GlobalConfiguration isn't initialized");
    }
    return m_cacheSize;
}

char *GlobalConfiguration::journalPath() const
{
    if (!m_isInitialized) {
	throw std::string("GlobalConfiguration isn't initialized");
    }
    return m_journalPath;
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
    if (read(fd, &m_rootNodePageNumber, sizeof(m_rootNodePageNumber)) != sizeof(m_rootNodePageNumber)) {
	throw std::string("Error reading global configuration");
    }
    if (read(fd, &m_cacheSize, sizeof(m_cacheSize)) != sizeof(m_cacheSize)) {
	throw std::string("Error reading global configuration");
    }
    size_t journalPathSize;
    if (read(fd, &journalPathSize, sizeof(journalPathSize)) != sizeof(journalPathSize)) {
	throw std::string("Error reading global configuration");
    }
    free(m_journalPath);
    m_journalPath = static_cast<char *>(malloc(journalPathSize));
    if (read(fd, m_journalPath, journalPathSize) != journalPathSize) {
	throw std::string("Error reading global configuration");
    }
}

void GlobalConfiguration::setRootNodePageNumber(const size_t &newRootNodePageNumber)
{
    m_rootNodePageNumber = newRootNodePageNumber;
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
    totalSeek += sizeof(m_rootNodePageNumber);
    totalSeek += sizeof(m_cacheSize);
    totalSeek += sizeof(size_t); // journal path size
    totalSeek += (strlen(m_journalPath) + 1) * sizeof(*m_journalPath);
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
    page.write(&m_rootNodePageNumber, sizeof(m_rootNodePageNumber));
    page.write(&m_cacheSize, sizeof(m_cacheSize));
    size_t journalPathSize = (strlen(m_journalPath) + 1) * sizeof(*m_journalPath);
    page.write(&journalPathSize, sizeof(journalPathSize));
    page.write(m_journalPath, journalPathSize);
}
