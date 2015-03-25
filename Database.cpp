#include "Database.h"

#include <cstring>
#include <memory>

// #include <iostream>
// #include <cassert>

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
//     std::cerr << "Insert begin\n";
    if (m_rootNode->keyCount() == 2 * T - 1) {
// 	std::cerr << "Inserting full\n";
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
// 	std::cerr << "Inserting not full\n";
	insertNonFull(m_rootNode, key, value);
    }

    DatabaseNode::Record trash;
    if (!select(key, trash)) {
	exit(0);
    }
}

void Database::insertNonFull(DatabaseNode *node, const DatabaseNode::Record &key, const DatabaseNode::Record &value)
{
    DatabaseNode::Record trash;

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

// 	if (!selectFromNode(node, key, trash)) {
// 	    std::cerr << "GFUCK 1\n";
// 	}
    } else {
// 	std::cerr << "going deeper\n";
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
// 	    std::cerr << "splitting\n";
	    splitChild(node, i, child);
// 	    assert(i < node->keyCount());
	    if (key > node->keys()[i]) {
// 		std::cerr << "qweqw\n";
		i++;
	    }
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

// 	if (!selectFromNode(child, key, trash)) {
// 	    std::cerr << "GFUCK 2\n";
// 	}
// 	size_t page = child->rootPage();

	child->writeToPages(&m_globConfiguration, m_pageReadWriter);
	delete child;

// 	child = new DatabaseNode(
// 	    &m_globConfiguration,
// 	    m_pageReadWriter,
// 	    page,
// 	    true);
//
// // 	if (!selectFromNode(node, key, trash)) {
// 	    std::cerr << "GFUCK 3\n";
// 	    for (int i = 0; i <= node->keyCount(); i++) {
// // 		std::cerr << node->linkedNodesRootPageNumbers()[i] << ' ';
// 		if (i < node->keyCount()) {
// 		    std::cerr << "[" << node->keys()[i].size << ", " << node->keys()[i].data << "] ";
// 		}
// 	    }
// 	    std::cerr << "\nexpect " << page << std::endl;
// 	    std::cerr << "key [" << key.size << ", " << key.data << "] " << std::endl;
// 	}
//
// 	if (!selectFromNode(child, key, trash)) {
// 	    std::cerr << "GFUCK 5\n";
// 	}
    }
}

void Database::splitChild(DatabaseNode *x, size_t i, DatabaseNode *y)
{
//     assert(y->keyCount() == 2 * T - 1);
//     assert(x->keyCount() != 2 * T - 1);
//     assert(!x->isLeaf());

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

// TODO
void Database::remove(const DatabaseNode::Record &key)
{

}

// To be implemented
void Database::sync()
{
}

bool Database::selectFromNode(DatabaseNode *node, const DatabaseNode::Record &key, DatabaseNode::Record &toWrite)
{
//     std::cerr << "selectFromNode " << static_cast<void *>(node) << ' ' << node->keyCount() << std::endl;
//     std::cerr << node->linkedNodesRootPageNumbers().size() << std::endl;
//     if (!node->isLeaf()) {
// 	for (size_t i = 0; i <= node->keyCount(); i++) {
// 	    std::cerr << node->linkedNodesRootPageNumbers()[i] << ' ';
// 	}
// 	std::cerr << std::endl;
//     }

    size_t i = 0;
    while (i < node->keyCount() && key > node->keys()[i]) { // Seek first >= key
	i++;
    }

//     std::cerr << "current i " << i << std::endl;

    if (i < node->keyCount() && key == node->keys()[i]) {
// 	std::cerr << "found\n";
	toWrite.size = node->data()[i].size;
	toWrite.data = new char[toWrite.size];
	memcpy(toWrite.data, node->data()[i].data, toWrite.size);
	return true;
    }

    if (node->isLeaf()) {
// 	std::cerr << "Not found leaf\n";
	return false;
    } else {
// 	std::cerr << "going down\n";
	size_t pageToGo = node->linkedNodesRootPageNumbers()[i];
	if (pageToGo == DatabaseNode::NO_PAGE) { // no such son
// 	    std::cerr << "Not found nowhere to go\n";
	    return false;
	}
// 	std::cerr << "reading next node\n";
	std::unique_ptr<DatabaseNode> nextNode(
	    new DatabaseNode(
		&m_globConfiguration,
		m_pageReadWriter,
		pageToGo,
		true
	    )
	);
// 	std::cerr << "stepping in\n";
	return selectFromNode(nextNode.get(), key, toWrite);
    }
}
