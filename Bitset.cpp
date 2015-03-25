#include "Bitset.h"

#include <cstdlib>
#include <string>
#include <cstring>
#include "Utils.h"

Bitset::Bitset()
    : m_isInitialised(false)
{
}

Bitset::~Bitset()
{
    if (m_isInitialised) {
        delete[] m_mask;
    }
}

size_t Bitset::maskSize() const
{
    return Utils::roundUpDiv<size_t>(m_globConf->pageCount(), 8);
}

size_t Bitset::indexPageCount() const
{
    return Utils::roundUpDiv(
	m_globConf->pageCount(),
	8 * m_globConf->pageSize()
    );
}

void Bitset::initialize(
    GlobalConfiguration *_globConf,
    const std::vector<size_t> &preallocatedPagesNum,
    size_t _indexStartingPage
)
{
    if (m_isInitialised) {
	throw std::string("Bitset is already intitialised");
    }
    m_isInitialised = true;

    m_globConf = _globConf;
    if (!m_globConf) {
	throw std::string("globConf can't be null");
    }

    m_indexStartingPage = _indexStartingPage;

    m_mask = new char[maskSize()];
    memset(m_mask, 0, maskSize());

    for (const size_t &i : preallocatedPagesNum) {
        set(i, true);
    }

    for (size_t i = 0; i < indexPageCount(); i++) {
	set(i + m_indexStartingPage, true);
    }
}

bool Bitset::get(const size_t &pos) const
{
    if (!m_isInitialised) {
        throw std::string("Bitset isn't initialized");
    }

    if (pos < 0 || pos >= m_globConf->pageCount()) {
        throw std::string("Invalid bitset position");
    }

    return (m_mask[pos / 8] >> (pos % 8)) & 1;
}

void Bitset::set(const size_t &pos, bool value)
{
    if (!m_isInitialised) {
        throw std::string("Bitset isn't initialised");
    }

    if (pos < 0 || pos >= m_globConf->pageCount()) {
        throw std::string("Invalid bitset position");
    }

    if (value) {
        m_mask[pos / 8] |= (1 << (pos % 8));
    } else {
        m_mask[pos / 8] &= ~(1 << (pos % 8));
    }
}

size_t Bitset::freePageNumber() const
{
    if (!m_isInitialised) {
        throw std::string("Bitset isn't initialised");
    }

    for (size_t i = 0; i < m_globConf->pageCount(); i++) {
        if (!get(i)) {
            return i;
        }
    }

    throw std::string("There is no free pages");
}

void Bitset::read(GlobalConfiguration *_globConf, Page &headerPage, PageReadWriter &rw)
{
    if (m_isInitialised) {
        throw std::string("Bitset is initialised");
    }
    m_isInitialised = true;

    m_globConf = _globConf;
    if (!m_globConf) {
	throw std::string("globConf can't be null");
    }

    headerPage.read(&m_indexStartingPage, sizeof(m_indexStartingPage));

    m_mask = new char[maskSize()];

    for (size_t i = 0; i < indexPageCount(); i++) {
        Page curPage(i + m_indexStartingPage, m_globConf->pageSize());
        rw.read(curPage);
        curPage.read(m_mask + i * m_globConf->pageSize(), m_globConf->pageSize());
    }
}

void Bitset::write(Page &headerPage, PageReadWriter &rw) const
{
    if (!m_isInitialised) {
        throw std::string("Bitset isn't initialised");
    }

    headerPage.write(&m_indexStartingPage, sizeof(m_indexStartingPage));

    for (size_t i = 0; i < indexPageCount(); i++) {
        Page curPage(i + m_indexStartingPage, m_globConf->pageSize());
        curPage.write(m_mask + i * m_globConf->pageSize(), m_globConf->pageSize());
        rw.write(curPage);
    }
}
