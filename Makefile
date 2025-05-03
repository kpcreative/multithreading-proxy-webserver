CC=gcc
CFLAGS= -g -Wall -pthread

all: proxy

proxy: proxy_server_with_cache.c proxy_parse.c
	$(CC) $(CFLAGS) -c proxy_parse.c -o proxy_parse.o
	$(CC) $(CFLAGS) -c proxy_server_with_cache.c -o proxy.o
	$(CC) $(CFLAGS) proxy_parse.o proxy.o -o proxy

clean:
	rm -f proxy *.o
