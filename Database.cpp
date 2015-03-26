#include "Database.h"

#include <cstring>
#include <memory>

const int Database::T = 10;

Database::Database(const char *databaseFile, const Database::Configuration &configuration)
    : m_globConfiguration(
	configuration.size / configuration.pageSize,
	configuration.pageSize,
	1) //desired params
    , m_pageReadWriter(databaseFile, &m_globConfiguration) // will init m_globConfiguration if file exists
    , m_rootNode(new DatabaseNode(
	&m_globConfiguration,
	m_pageReadWriter,
	m_globConfiguration.rootNodeFirstPageNumber(),
	m_globConfiguration.isReadedFromFile()))
{
}

Database::~Database()
{
    m_rootNode->writeToPages(&m_globConfiguration, m_pageReadWriter);
    close();
    delete m_rootNode;
}

void Database::close()
{
    m_pageReadWriter.close();
}

void Database::insert(const DatabaseNode::Record &key, const DatabaseNode::Record &value)
{
    if (m_rootNode->keyCount() == 2 * T - 1) {
	DatabaseNode *s = new DatabaseNode(
	    &m_globConfiguration,
	    m_pageReadWriter,
	    m_pageReadWriter.allocatePageNumber(),
	    false
	);

	s->setIsLeaf(false);
	s->setKeyCount(0);
	s->linkedNodesRootPageNumbers().push_back(m_rootNode->rootPage());

	splitChild(s, 0, m_rootNode);

	insertNonFull(s, key, value);

	m_rootNode->writeToPages(&m_globConfiguration, m_pageReadWriter);
	delete m_rootNode;

	m_rootNode = s;
    } else {
	insertNonFull(m_rootNode, key, value);
    }
}

void Database::insertNonFull(DatabaseNode *node, const DatabaseNode::Record &key, const DatabaseNode::Record &value)
{
    size_t i = node->keyCount() - 1;
    while (node->keyCount() && i >= 0 && key < node->keys()[i]) { // seeking last <= key
	if (i == 0) {
	    i--;
	    break;
	}
	i--;
    }
    if (i < node->keyCount() && node->keys()[i] == key) {
	delete[] node->data()[i].data;
	node->data()[i].size = value.size;
	node->data()[i].data = new char[value.size];
	memcpy(node->data()[i].data, value.data, value.size);
	return;
    }

    if (node->isLeaf()) {
	i++;
	node->keys().insert(node->keys().begin() + i, DatabaseNode::Record::rawCopyFrom(key));
	node->data().insert(node->data().begin() + i, DatabaseNode::Record::rawCopyFrom(value));
	node->setKeyCount(node->keyCount() + 1);
    } else {
	i++;
	DatabaseNode *child = 0;
	if (node->linkedNodesRootPageNumbers()[i] != DatabaseNode::NO_PAGE) {
	    child = new DatabaseNode(
		&m_globConfiguration,
		m_pageReadWriter,
		node->linkedNodesRootPageNumbers()[i],
		true
	    );
	} else {
	    child = new DatabaseNode(
		&m_globConfiguration,
		m_pageReadWriter,
		m_pageReadWriter.allocatePageNumber(),
		false);
	    node->linkedNodesRootPageNumbers()[i] = child->rootPage();
	}

	if (child->keyCount() == 2 * T - 1) {
	    splitChild(node, i, child);
	    if (key > node->keys()[i]) {
		i++;
	    }
	}

	if (i < node->keyCount() && node->keys()[i] == key) {
	    delete[] node->data()[i].data;
	    node->data()[i].size = value.size;
	    node->data()[i].data = new char[value.size];
	    memcpy(node->data()[i].data, value.data, value.size);
	    return;
	}

	child->writeToPages(&m_globConfiguration, m_pageReadWriter);
	delete child;

	if (node->linkedNodesRootPageNumbers()[i] != DatabaseNode::NO_PAGE) {
	    child = new DatabaseNode(
		&m_globConfiguration,
		m_pageReadWriter,
		node->linkedNodesRootPageNumbers()[i],
				     true
	    );
	} else {
	    child = new DatabaseNode(
		&m_globConfiguration,
		m_pageReadWriter,
		m_pageReadWriter.allocatePageNumber(),
				     false);
	    node->linkedNodesRootPageNumbers()[i] = child->rootPage();
	}

	insertNonFull(child, key, value);

	child->writeToPages(&m_globConfiguration, m_pageReadWriter);
	delete child;
    }
}

void Database::splitChild(DatabaseNode *x, size_t i, DatabaseNode *y)
{
    DatabaseNode *z = new DatabaseNode(
	&m_globConfiguration,
	m_pageReadWriter,
	m_pageReadWriter.allocatePageNumber(),
	false);
    std::unique_ptr<DatabaseNode> zUniq(z);

    z->setIsLeaf(y->isLeaf());
    z->setKeyCount(T - 1);

    z->keys().insert(z->keys().begin(), y->keys().begin() + T, y->keys().end());
    y->keys().erase(y->keys().begin() + T, y->keys().end());

    z->data().insert(z->data().begin(), y->data().begin() + T, y->data().end());
    y->data().erase(y->data().begin() + T, y->data().end());

    std::vector<size_t> &zLinks = z->linkedNodesRootPageNumbers();
    std::vector<size_t> &yLinks = y->linkedNodesRootPageNumbers();
    if (!y->isLeaf()) {
	zLinks.insert(zLinks.begin(), yLinks.begin() + T, yLinks.end());
	yLinks.erase(yLinks.begin() + T, yLinks.end());
    }

    x->keys().insert(x->keys().begin() + i, y->keys()[T - 1]);
    x->data().insert(x->data().begin() + i, y->data()[T - 1]);
    std::vector<size_t> &xLinks = x->linkedNodesRootPageNumbers();
    xLinks[i] = z->rootPage();
    xLinks.insert(xLinks.begin() + i, y->rootPage());
    x->setKeyCount(x->keyCount() + 1);

    y->keys().erase(y->keys().begin() + T - 1);
    y->data().erase(y->data().begin() + T - 1);
    y->setKeyCount(T - 1);

    x->writeToPages(&m_globConfiguration, m_pageReadWriter);
    y->writeToPages(&m_globConfiguration, m_pageReadWriter);
    z->writeToPages(&m_globConfiguration, m_pageReadWriter);
}

bool Database::select(const DatabaseNode::Record &key, DatabaseNode::Record &toWrite)
{
    return selectFromNode(m_rootNode, key, toWrite);
}

void Database::remove(const DatabaseNode::Record &key)
{
    DatabaseNode::Record trash;
    if (!select(key, trash)) {
	return;
    }
    removeFromNode(m_rootNode, key);
}

// To be implemented
void Database::sync()
{
}

bool spec = false;

bool Database::selectFromNode(DatabaseNode *node, const DatabaseNode::Record &key, DatabaseNode::Record &toWrite)
{
    size_t i = 0;
    while (i < node->keyCount() && key > node->keys()[i]) { // Seek first >= key
	i++;
    }
    if (i < node->keyCount() && key == node->keys()[i]) {
	toWrite.size = node->data()[i].size;
	toWrite.data = new char[toWrite.size];
	memcpy(toWrite.data, node->data()[i].data, toWrite.size);
	return true;
    }
    if (node->isLeaf()) {
	return false;
    } else {
	size_t pageToGo = node->linkedNodesRootPageNumbers()[i];
	if (pageToGo == DatabaseNode::NO_PAGE) { // no such son
	    return false;
	}
	std::unique_ptr<DatabaseNode> nextNode(
	    new DatabaseNode(
		&m_globConfiguration,
		m_pageReadWriter,
		pageToGo,
		true
	    )
	);
	return selectFromNode(nextNode.get(), key, toWrite);
    }
}

void Database::removeFromNode(DatabaseNode *x, const DatabaseNode::Record &key)
{
    size_t i = 0;
    while (i < x->keyCount() && key > x->keys()[i]) { // Seek first >= key
	i++;
    }
    if (x->isLeaf()) { // Case 1
	if (i < x->keyCount() && key == x->keys()[i]) {
	    delete[] x->keys()[i].data;
	    delete[] x->data()[i].data;
	    x->keys().erase(x->keys().begin() + i);
	    x->data().erase(x->data().begin() + i);
	    x->setKeyCount(x->keyCount() - 1);
	} else {
	    throw std::string("No such key to remove.");
	}
    } else if (i < x->keyCount() && key == x->keys()[i]) { // Case 2
	DatabaseNode *y = loadFromDiskOrCreate(x->linkedNodesRootPageNumbers()[i]);
	DatabaseNode *z = loadFromDiskOrCreate(x->linkedNodesRootPageNumbers()[i + 1]);
	std::vector<size_t> &xLinks = x->linkedNodesRootPageNumbers();

	if (y->keyCount() > T - 1) { // Case 2 a
	    DatabaseNode::Record replacingKey, replacingData;
	    findRightmostKey(y, replacingKey, replacingData);
	    removeFromNode(y, replacingKey);

	    delete[] x->keys()[i].data;
	    delete[] x->data()[i].data;
	    x->keys()[i] = replacingKey;
	    x->data()[i] = replacingData;

	    y->writeToPages(&m_globConfiguration, m_pageReadWriter);
	    z->writeToPages(&m_globConfiguration, m_pageReadWriter);
	    delete y;
	    delete z;
	} else if (z->keyCount() > T - 1) { // Case 2 b
	    DatabaseNode::Record replacingKey, replacingData;
	    findLeftmostKey(z, replacingKey, replacingData);
	    removeFromNode(z, replacingKey);

	    delete[] x->keys()[i].data;
	    delete[] x->data()[i].data;

	    x->keys()[i] = replacingKey;
	    x->data()[i] = replacingData;

	    y->writeToPages(&m_globConfiguration, m_pageReadWriter);
	    z->writeToPages(&m_globConfiguration, m_pageReadWriter);
	    delete y;
	    delete z;
	} else { // Case 2 c
	    merge(y, x, i, z);
	    removeFromNode(y, key);
	    y->writeToPages(&m_globConfiguration, m_pageReadWriter);
	    delete y;
	    delete z;
	}
    } else { // Case 3
	bool writeY = true;
	DatabaseNode *y = loadFromDiskOrCreate(x->linkedNodesRootPageNumbers()[i]);
	if (y->keyCount() > T - 1) {
	    removeFromNode(y, key);
	    y->writeToPages(&m_globConfiguration, m_pageReadWriter);
	    delete y;
	    return;
	}

	DatabaseNode *yLeft = 0;
	if (i >= 1) {
	    yLeft = loadFromDiskOrCreate(x->linkedNodesRootPageNumbers()[i - 1]);
	}

	bool writeRight = true;
	DatabaseNode *yRight = 0;
	if (i + 1 <= x->keyCount()) {
	    yRight = loadFromDiskOrCreate(x->linkedNodesRootPageNumbers()[i + 1]);
	}

	if (yLeft && yLeft->keyCount() > T - 1) {
	    y->keys().insert(y->keys().begin(), x->keys()[i - 1]);
	    y->data().insert(y->data().begin(), x->data()[i - 1]);
	    y->setKeyCount(y->keyCount() + 1);

	    x->keys()[i - 1] = yLeft->keys().back();
	    x->data()[i - 1] = yLeft->data().back();

	    yLeft->keys().pop_back();
	    yLeft->data().pop_back();
	    if (!yLeft->isLeaf()) {
		y->linkedNodesRootPageNumbers().insert(
		    y->linkedNodesRootPageNumbers().begin(),
		    yLeft->linkedNodesRootPageNumbers().back());

		yLeft->linkedNodesRootPageNumbers().pop_back();
	    }
	    yLeft->setKeyCount(yLeft->keyCount() - 1);

	    removeFromNode(y, key);
	} else if (yRight && yRight->keyCount() > T - 1) {

	    y->keys().push_back(x->keys()[i]);
	    y->data().push_back(x->data()[i]);
	    y->setKeyCount(y->keyCount() + 1);

	    x->keys()[i] = yRight->keys()[0];
	    x->data()[i] = yRight->data()[0];

	    yRight->keys().erase(yRight->keys().begin());
	    yRight->data().erase(yRight->data().begin());
	    if (!yRight->isLeaf()) {
		y->linkedNodesRootPageNumbers().push_back(yRight->linkedNodesRootPageNumbers()[0]);
		yRight->linkedNodesRootPageNumbers().erase(yRight->linkedNodesRootPageNumbers().begin());
	    }
	    yRight->setKeyCount(yRight->keyCount() - 1);

	    removeFromNode(y, key);
	} else if (yLeft) {
	    merge(yLeft, x, i - 1, y);
	    writeY = false;
	    removeFromNode(yLeft, key);
	} else if (yRight) {
	    merge(y, x, i, yRight);
	    writeRight = false;
	    removeFromNode(y, key);
	}

	if (writeY) {
	    y->writeToPages(&m_globConfiguration, m_pageReadWriter);
	}
	delete y;

	if (yLeft) {
	    yLeft->writeToPages(&m_globConfiguration, m_pageReadWriter);
	    delete yLeft;
	}

	if (yRight) {
	    if (writeRight) {
		yRight->writeToPages(&m_globConfiguration, m_pageReadWriter);
	    }
	    delete yRight;
	}
    }
}

DatabaseNode *Database::loadFromDiskOrCreate(size_t &pageNum)
{
    if (pageNum == DatabaseNode::NO_PAGE) {
	pageNum = m_pageReadWriter.allocatePageNumber();
	return new DatabaseNode(
	    &m_globConfiguration,
	    m_pageReadWriter,
	    pageNum,
	    false);
    } else {
	return new DatabaseNode(
	    &m_globConfiguration,
	    m_pageReadWriter,
	    pageNum,
	    true);
    }
}

void Database::findLeftmostKey(DatabaseNode *node, DatabaseNode::Record &key, DatabaseNode::Record &value)
{
    if (node->isLeaf()) {
	key = DatabaseNode::Record::rawCopyFrom(node->keys()[0]);
	value =  DatabaseNode::Record::rawCopyFrom(node->data()[0]);
    } else {
	DatabaseNode *toGo = loadFromDiskOrCreate(node->linkedNodesRootPageNumbers()[0]);
	findLeftmostKey(toGo, key, value);
	delete toGo;
    }
}

void Database::findRightmostKey(DatabaseNode *node, DatabaseNode::Record &key, DatabaseNode::Record &value)
{
    if (node->isLeaf()) {
	key = DatabaseNode::Record::rawCopyFrom(node->keys().back());
	value = DatabaseNode::Record::rawCopyFrom(node->data().back());
    } else {
	DatabaseNode *toGo = loadFromDiskOrCreate(node->linkedNodesRootPageNumbers().back());
	findRightmostKey(toGo, key, value);
	delete toGo;
    }
}

void Database::merge(DatabaseNode *y, DatabaseNode *x, size_t i, DatabaseNode *z)
{
    y->keys().push_back(x->keys()[i]);
    y->data().push_back(x->data()[i]);
    x->keys().erase(x->keys().begin() + i);
    x->data().erase(x->data().begin() + i);

    y->keys().insert(y->keys().end(), z->keys().begin(), z->keys().end());
    z->keys().clear();
    y->data().insert(y->data().end(), z->data().begin(), z->data().end());
    z->data().clear();

    if (!y->isLeaf()) {
	std::vector<size_t> &yLinks = y->linkedNodesRootPageNumbers();
	std::vector<size_t> &zLinks = z->linkedNodesRootPageNumbers();

	yLinks.insert(yLinks.end(), zLinks.begin(), zLinks.end());
	zLinks.clear();
    }

    std::vector<size_t> &xLinks = x->linkedNodesRootPageNumbers();
    xLinks[i + 1] = y->rootPage();
    xLinks.erase(xLinks.begin() + i);

    x->setKeyCount(x->keyCount() - 1);
    y->setKeyCount(y->keyCount() + 1 + z->keyCount());
    z->setKeyCount(0);
    z->freePages(m_pageReadWriter);
}
