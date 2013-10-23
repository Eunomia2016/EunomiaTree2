#ifndef PBUF_H
#define PBUF_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>


class PBuf {

private:
	
	
public:

	PBuf();
	
	~PBuf();
	
	void WriteBytes(int len, void *data);

	void Print();
	
};


#endif
