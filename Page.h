#pragma once

#include <cstddef>

class Page
{
public:
    Page(const size_t &number, const size_t &pageSize);
    ~Page();

    size_t number() const;
    void seek(size_t pos);
    void seekForward(size_t totalSeek);
    void write(const void *writeData, const size_t &size);
    void read(void *readData, const size_t &size);
    size_t freeSpace() const;

    char *rawData() const;

private:
    char *m_data;
    size_t m_pageSize;
    size_t m_number;
    size_t m_cursorPos;

    Page(const Page &) { }
    void operator=(const Page &p) { }
};
