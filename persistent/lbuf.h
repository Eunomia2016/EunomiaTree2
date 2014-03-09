#ifndef LBUF_H
#define LBUF_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include "log.h"


#define BSIZE 64*1024 //64KB

class LocalPBuf {

	struct PEntry {
		int tableid;
		uint64_t key;
		uint64_t seqno;
		uint64_t* value;
		int vlen;
	};

public:

	uint64_t sn;
	int cur;
	PEntry buf[BSIZE];



	LocalPBuf* next;
		
	LocalPBuf() {
		next = NULL;
		sn = 0;
		cur = 0;
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
		buf[cur].tableid = tabid;
		buf[cur].key = key;
		buf[cur].seqno = seqno;
		buf[cur].value = value;
		buf[cur].vlen = vlen;

		cur++;
	}

	void Reset() {
		
		cur = 0;
		sn = 0;
		next = NULL;
	}

	int Serialize(Log* lf) {

		int len = 0;
		for(int i = 0; i < cur; i++) {
			lf->writeLog((char *)&buf[i].tableid, sizeof(int));
			lf->writeLog((char *)&buf[i].key, sizeof(uint64_t));
			lf->writeLog((char *)&buf[i].seqno, sizeof(uint64_t));
			
			if(buf[i].value == NULL) {
				uint64_t nulval = -1;
				lf->writeLog((char *)&nulval, sizeof(uint64_t));
			} else {
				lf->writeLog((char *)buf[i].value, buf[i].vlen);
			}
			len += 4+8+8+buf[i].vlen;
		}

		return len;
	}
	
};

#endif
