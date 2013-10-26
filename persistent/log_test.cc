#include "log.h"

int main() {
	char * path = "test.txt";
	Log log(path);

	for(int i = 0; i < 100; i++) {
		char c = 'a'+i % 24;
		log.writeLog((char *)&c, sizeof(c));
	}

	log.closelog();
	
	return 1;
}
