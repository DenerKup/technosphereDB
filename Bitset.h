#pragma once

#include "Page.h"
#include "PageReadWriter.h"
#include "GlobalConfiguration.h"

#include <vector>

class Bitset
{
public:
    Bitset();
    ~Bitset();

    void initialize(GlobalConfiguration *globConf, const std::vector<size_t> &preallocatedPagesNum, size_t indexStartingPage);

    bool get(const size_t &pos) const;
    void set(const size_t &pos, bool value);
    void read(GlobalConfiguration *globConf, Page &headerPage, PageReadWriter &rw);
    void write(Page &headerPage, PageReadWriter &rw) const;
    size_t freePageNumber() const;

private:
    bool m_isInitialised;
    GlobalConfiguration *m_globConf;

    char *m_mask;
    size_t m_indexStartingPage;

    size_t maskSize() const;
    size_t indexPageCount() const;

    Bitset(const Bitset &) { }
    void operator=(const Bitset &) { }
};
