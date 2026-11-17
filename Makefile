CC = mpicc
CFLAGS = -O2 -Wall -Wextra -std=c11

all: busca_logs

busca_logs: busca_logs.c
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -f busca_logs

.PHONY: all clean