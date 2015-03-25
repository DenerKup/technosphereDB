#pragma once

#include "Page.h"

class PageReadWriter
{
public:
    /// Returns free page number
    virtual size_t allocatePageNumber() = 0;
    /// Deallocates page number
    virtual void deallocatePageNumber(const size_t &number) = 0;
    /// Reads page to memory
    virtual void read(Page &page) = 0;
    /// Writes page to storage
    virtual void write(const Page &page) = 0;
    /// Closes read/write flow
    virtual void close() = 0;
    /// Flushes changes
    virtual void flush() = 0;
};
