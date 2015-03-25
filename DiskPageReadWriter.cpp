#include "DiskPageReadWriter.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <cstring>

#include <errno.h>
#include <iostream>

DiskPageReadWriter::DiskPageReadWriter(const char* file, GlobalConfiguration *_globConf)
    : m_fd(-1)
    , m_globConf(_globConf)
{
    if (!m_globConf) {
	throw std::string("globConf can't be null");
    }
    //check file existence
    if (access(file, F_OK) == -1) { // No file
	m_globConf->initialize(
	    m_globConf->desiredPageCount(),
	    m_globConf->desiredPageSize(),
	    m_globConf->desiredRootNodeFirstPageNumber());

	m_fd = creat(file, 0644);
	if (m_fd == -1) {
	    throw std::string("Error creating file");
	}

	if (ftruncate(m_fd, m_globConf->databaseSize()) == -1) {
	    ::close(m_fd);
	    throw std::string("Error resizing file to needed size");
	}

	// Preallocating global configuration page and root node first page >
	m_bitset.initialize(
	    m_globConf,
	    {0, m_globConf->rootNodeFirstPageNumber()},
	    m_globConf->rootNodeFirstPageNumber() + 1
	);
	writeGlobConfAndBitset();

	::close(m_fd);
	m_fd = open(file, O_RDWR);
    } else {
	m_fd = open(file, O_RDWR);
	if (m_fd == -1) {
	    throw std::string("Error opening file");
	}

	m_globConf->readFromFile(m_fd);

	Page firstPage(0, m_globConf->pageSize());
	read(firstPage);
	firstPage.seek(0);
	m_globConf->skipDataOnPage(firstPage);

	m_bitset.read(m_globConf, firstPage, *this);
    }
}

void DiskPageReadWriter::writeGlobConfAndBitset()
{
    Page firstPage(0, m_globConf->pageSize());
    firstPage.seek(0);
    m_globConf->writeToPage(firstPage);
    m_bitset.write(firstPage, *this);
    write(firstPage);
}

size_t DiskPageReadWriter::allocatePageNumber()
{
    size_t res = m_bitset.freePageNumber();
    m_bitset.set(res, 1);
    return res;
}

void DiskPageReadWriter::deallocatePageNumber(const size_t &number)
{
    m_bitset.set(number, 0);
}

void DiskPageReadWriter::read(Page &p)
{
    if (p.number() >= m_globConf->pageCount()) {
	throw std::string("Invalid page number\n");
    }

    std::cerr << "Reading page number " << p.number() << ' ' << m_fd << std::endl;

    if (lseek(m_fd, p.number() * m_globConf->pageSize(), SEEK_SET) == -1) {
	throw std::string("Error seeking page");
    }
    if (::read(m_fd, p.rawData(), m_globConf->pageSize()) != m_globConf->pageSize()) {
	std::cerr << strerror(errno) << std::endl;
	throw std::string("Error reading page");
    }
    std::cerr << "Reading page end\n";
}

void DiskPageReadWriter::write(const Page &p)
{
    if (p.number() >= m_globConf->pageCount()) {
	throw std::string("Invalid page number\n");
    }
//     std::cerr << "writting page " << p.number() << std::endl;
    if (lseek(m_fd, p.number() * m_globConf->pageSize(), SEEK_SET) == -1) {
	throw std::string("Error seeking page");
    }
    if (::write(m_fd, p.rawData(), m_globConf->pageSize()) != m_globConf->pageSize()) {
	throw std::string("Error writing page");
    }
//     std::cerr << "s page " << p.number() << std::endl;
}

void DiskPageReadWriter::flush()
{
    writeGlobConfAndBitset();
}

void DiskPageReadWriter::close()
{
    if (m_fd != -1) {
	flush();
	if (::close(m_fd) == -1) {
	    throw std::string("Error closing file");
	}
    }
}
