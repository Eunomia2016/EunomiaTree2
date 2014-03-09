#ifndef LBUF_H
#define LBUF_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include <string.h>
#include "log.h"


#define BSIZE 128*1024*1024 //64MB

class LocalPBuf {

public:
	
	uint64_t sn;
	int cur;
	char *buf;

	LocalPBuf* next;
		
	LocalPBuf() {

		next = NULL;
		sn = 0;
		cur = 0;
		
		buf = new char[BSIZE];
	}

	int EmptySlotNum() {
		return BSIZE - cur;
	}

	uint64_t GetSN() {
		return sn;
	}

	void SetSN(uint64_t s) {
		sn = s;
	}

	void PutRecord(int tabid, uint64_t key, 
		uint64_t seqno, uint64_t* value, int vlen)
	{
			memcpy(&buf[cur], (char *)&tabid, sizeof(int));
			cur += sizeof(int);
			
			memcpy(&buf[cur], (char *)&key, sizeof(uint64_t));
			cur += sizeof(uint64_t);
			
			memcpy(&buf[cur], (char *)&seqno, sizeof(uint64_t));
			cur += sizeof(uint64_t);

			if(value == NULL) {
				uint64_t nulval = -1;
				memcpy(&buf[cur], (char *)&nulval, sizeof(uint64_t));
				cur += sizeof(uint64_t);
			} else {
				memcpy(&buf[cur], (char *)&value, vlen);
				cur += vlen;
			}
			
			assert(cur < BSIZE);
	}

	void Reset() {
		
		cur = 0;
		sn = 0;
		next = NULL;
	}

	int Serialize(Log* lf) {

		lf->writeLog(buf, cur);
		return cur;
	}
	
};

#endif
