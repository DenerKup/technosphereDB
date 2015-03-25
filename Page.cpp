#include "Page.h"

#include <cstring>
#include <string>
#include <iostream>

Page::Page(const size_t &number, const size_t &pageSize)
    : m_data(0)
    , m_pageSize(pageSize)
    , m_number(number)
    , m_cursorPos(0)
{
    m_data = new char[pageSize];
    memset(m_data, 0, pageSize);
}

Page::~Page()
{
    delete[] m_data;
}

size_t Page::number() const
{
    return m_number;
}

void Page::seek(size_t pos)
{
    m_cursorPos = pos;
}

void Page::seekForward(size_t totalSeek)
{
    m_cursorPos += totalSeek;
}

void Page::read(void *_readData, const size_t &size)
{
    char *readData = reinterpret_cast<char *>(_readData);
    if (m_cursorPos + size > m_pageSize || size == 0) {
	throw std::string("Invalid read from page");
    }
    memcpy(readData, m_data + m_cursorPos, size);
    m_cursorPos += size;
}

void Page::write(const void *_writeData, const size_t &size)
{
//     std::cerr << "START " << size << std::endl;;
    const char *writeData = reinterpret_cast<const char *>(_writeData);
//     std::cerr << "| " <<  static_cast<const void *>(writeData) << std::endl;
    if (m_cursorPos + size > m_pageSize || size == 0) {
	throw std::string("Invalid write to page");
    }
//     std::cerr << "| " << static_cast<void *>(m_data) << ' ' << m_cursorPos << ' ' << static_cast<const void *>(writeData) << std::endl;
    memcpy(m_data + m_cursorPos, writeData, size);
//     std::cerr << "|\n";
    m_cursorPos += size;
//     std::cerr << "END\n";
}

size_t Page::freeSpace() const
{
    return m_pageSize - m_cursorPos;
}

char *Page::rawData() const
{
    return m_data;
}
