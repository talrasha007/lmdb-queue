#include <string>

#include <lmdb/lmdb.h>

class Producer {
public:
	Producer(const std::string& root);
	~Producer();

private:
	std::string _root;
};
