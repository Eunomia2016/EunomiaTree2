#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "log.h"
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>



Log::Log(const char* p, bool sync): path(p)
{ 
	length = CHUNCKSIZE;
	//XXX: p shouldn't exist
	fd = open(path, O_RDWR|O_CREAT);
	
	if(fd < 0) {
		perror("LOG ERROR: create log file\n");
		exit(1);
	}
	
	ftruncate(fd, length);

	buf.start = (char *)mmap (0, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

	if(MAP_FAILED == buf.start) {
		perror("LOG ERROR: mmap in LOG\n");
		exit(1);
	}

	buf.cur = buf.start;
	buf.end = buf.start + length;
	


	if (madvise(buf.start, length, MADV_SEQUENTIAL) == -1) {
        perror("LOG ERROR: madvise in LOG\n");
        exit(1);
    }
}

Log::~Log()
{
	closelog();
}

void Log::enlarge(int inc_size)
{
	assert(inc_size % PAGESIZE == 0);
	
	if (munmap(buf.start, buf.end - buf.start) == -1) {
		perror("LOG ERROR: munmap in enlarge");
        exit(1);
	}

	struct stat sb;
	if (fstat(fd, &sb) == -1) {
        perror("LOG ERROR: fstat in enlarge\n");
        exit(1);
    }
	
    
    off_t original_size = sb.st_size;
	
    assert(original_size != 0 && original_size % PAGESIZE == 0);
	
    if (ftruncate(fd, original_size + inc_size) == -1) {
        perror("LOG ERROR: ftruncate in enlarge\n");
        exit(1);
    }

	int map_size = inc_size + PAGESIZE;
	
    buf.start = (char *)mmap(0, map_size, PROT_WRITE|PROT_READ, MAP_SHARED,
        	fd, original_size - PAGESIZE);

	long page_offset = (long)buf.cur & 0xFFF;
	buf.cur = buf.start + (page_offset ? page_offset : PAGESIZE);

	
#ifdef DEBUG
		fprintf(stderr, "new start: %p buf: %p truncate to %ld bytes\n", 
			buf.start, buf.cur, (long)(original_size + inc_size));
#endif
	
		if (madvise(buf.start, map_size, MADV_SEQUENTIAL) == -1) {
			perror("LOG ERROR: madvise in enlarge");
			exit(1);
		}
    
}

void Log::writeLog(char *data, int len)
{
	if((buf.cur + len) > buf.end)
		enlarge(CHUNCKSIZE);

	memcpy(buf.cur, data, len);

	buf.cur += len;
}

void Log::closelog()
{
	if (munmap(buf.start, length) == -1) {
		perror("LOG ERROR: munmap in close");
        exit(1);
	}

	close(fd);
	
}

